use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};

use crate::catalog::StreamCatalog;
use crate::config::DecoderConfig;
use crate::decoder::{DecodedMessage, StaticProstDecoderRegistry};
use crate::export::{session_jsonl, session_markdown_summary};
use crate::projection::{LogicalProjectionSummary, ProjectionStore};
use crate::session::{Session, SessionArchiveSummary};
use crate::stream_id::StreamId;
use crate::ui::model::{selected_logical_name, ActivePanel, UiState};

#[derive(Debug, Clone, PartialEq)]
pub enum TimelineEntry {
    Message(DecodedMessage),
    SessionBoundary {
        session_number: u64,
        started_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone)]
pub struct ArchivedTimeline {
    pub session_number: u64,
    pub started_at: DateTime<Utc>,
    pub messages: Vec<DecodedMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppCommand {
    Quit,
    NewSession,
    SelectNext,
    SelectPrevious,
    OpenSelected,
    Back,
    ToggleShardView,
    Refresh,
    AddMarker,
    BookmarkSelected,
    ExportSession,
    HalfPageDown,
    HalfPageUp,
    OpenLogs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerEvent {
    MessageObserved(crate::decoder::RawStreamMessage),
    GroupMetricUpdated(crate::projection::GroupMetric),
    PendingSummaryUpdated(crate::projection::PendingSummary),
    WorkerWarning(String),
    WorkerError(String),
}

pub struct App {
    pub catalog: StreamCatalog,
    pub projections: ProjectionStore,
    decoder_registry: StaticProstDecoderRegistry,
    pub ui: UiState,
    pub warnings: Vec<String>,
    pub archives: Vec<SessionArchiveSummary>,
    pub archived_timelines: Vec<ArchivedTimeline>,
    pub markers: Vec<SessionMarker>,
    pub bookmarks: Vec<MessageBookmark>,
    pub export_dir: PathBuf,
    pub message_view_rows: usize,
    pub log_view_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMarker {
    pub at: DateTime<Utc>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBookmark {
    pub stream: String,
    pub logical_stream: String,
    pub shard: Option<u16>,
    pub id: StreamId,
    pub message_type: String,
    pub at: DateTime<Utc>,
}

impl App {
    pub fn new(catalog: StreamCatalog, baselines: BTreeMap<String, StreamId>) -> Self {
        Self::new_with_decoders(catalog, baselines, BTreeMap::new())
    }

    pub fn new_with_decoders(
        catalog: StreamCatalog,
        baselines: BTreeMap<String, StreamId>,
        decoders: BTreeMap<String, DecoderConfig>,
    ) -> Self {
        let session = Session::new_numbered("session", 1, baselines);
        let projections = ProjectionStore::new(catalog.clone(), session, 1000);
        Self {
            catalog,
            projections,
            decoder_registry: StaticProstDecoderRegistry::from_configs(decoders),
            ui: UiState::new(),
            warnings: Vec::new(),
            archives: Vec::new(),
            archived_timelines: Vec::new(),
            markers: Vec::new(),
            bookmarks: Vec::new(),
            export_dir: PathBuf::from("exports"),
            message_view_rows: 10,
            log_view_rows: 10,
        }
    }

    pub fn with_zero_baselines(catalog: StreamCatalog) -> Self {
        let baselines = catalog
            .physical_streams()
            .into_iter()
            .map(|stream| (stream.name.clone(), StreamId::ZERO))
            .collect();
        Self::new(catalog, baselines)
    }

    pub fn summaries(&self) -> Vec<LogicalProjectionSummary> {
        self.projections.logical_summaries()
    }

    pub fn selected_timeline(&self) -> Vec<DecodedMessage> {
        let summaries = self.summaries();
        let Some(logical_name) = selected_logical_name(&summaries, &self.ui) else {
            return Vec::new();
        };

        let mut messages = Vec::new();
        for archived in &self.archived_timelines {
            messages.extend(
                archived
                    .messages
                    .iter()
                    .filter(|message| message.logical_stream == logical_name)
                    .cloned(),
            );
        }
        if let Ok(current) = self.projections.timeline(&logical_name) {
            messages.extend(current);
        }
        messages
    }

    pub fn selected_timeline_entries(&self) -> Vec<TimelineEntry> {
        let summaries = self.summaries();
        let Some(logical_name) = selected_logical_name(&summaries, &self.ui) else {
            return Vec::new();
        };

        let mut entries = Vec::new();
        for archived in &self.archived_timelines {
            if archived.session_number > 1 {
                entries.push(TimelineEntry::SessionBoundary {
                    session_number: archived.session_number,
                    started_at: archived.started_at,
                });
            }
            entries.extend(
                archived
                    .messages
                    .iter()
                    .filter(|message| message.logical_stream == logical_name)
                    .cloned()
                    .map(TimelineEntry::Message),
            );
        }

        let current_session = self.projections.session();
        if current_session.number > 1 {
            entries.push(TimelineEntry::SessionBoundary {
                session_number: current_session.number,
                started_at: current_session.started_at,
            });
        }
        if let Ok(messages) = self.projections.timeline(&logical_name) {
            entries.extend(messages.into_iter().map(TimelineEntry::Message));
        }

        entries
    }

    pub fn handle_worker_event(&mut self, event: WorkerEvent) {
        match event {
            WorkerEvent::MessageObserved(message) => {
                if let Err(error) = self
                    .projections
                    .observe_with_registry(message, &self.decoder_registry)
                {
                    self.warnings.push(error.to_string());
                }
            }
            WorkerEvent::GroupMetricUpdated(metric) => {
                self.projections.update_group_metric(metric);
            }
            WorkerEvent::PendingSummaryUpdated(summary) => {
                self.projections.update_pending_summary(summary);
            }
            WorkerEvent::WorkerWarning(warning) | WorkerEvent::WorkerError(warning) => {
                self.warnings.push(warning);
            }
        }
    }

    pub fn handle_command(&mut self, command: AppCommand) -> bool {
        match command {
            AppCommand::Quit => return false,
            AppCommand::NewSession => self.start_new_session(),
            AppCommand::SelectNext => {
                if self.ui.active_panel == ActivePanel::StreamDetail {
                    let count = self.selected_timeline().len();
                    self.ui.select_next_message(count, self.message_view_rows);
                } else if self.ui.active_panel == ActivePanel::Logs {
                    self.ui
                        .scroll_logs_down(self.warnings.len(), self.log_view_rows, 1);
                } else {
                    let count = self.summaries().len();
                    self.ui.select_next(count);
                }
            }
            AppCommand::SelectPrevious => {
                if self.ui.active_panel == ActivePanel::StreamDetail {
                    let count = self.selected_timeline().len();
                    self.ui
                        .select_previous_message(count, self.message_view_rows);
                } else if self.ui.active_panel == ActivePanel::Logs {
                    self.ui.scroll_logs_up(1);
                } else {
                    self.ui.select_previous();
                }
            }
            AppCommand::OpenSelected => {
                self.ui.active_panel = ActivePanel::StreamDetail;
                self.ui.reset_message_scroll();
            }
            AppCommand::Back => self.ui.active_panel = ActivePanel::Dashboard,
            AppCommand::ToggleShardView => self.ui.show_shards = !self.ui.show_shards,
            AppCommand::Refresh => {}
            AppCommand::AddMarker => self.markers.push(SessionMarker {
                at: Utc::now(),
                text: "manual marker".to_string(),
            }),
            AppCommand::BookmarkSelected => self.bookmark_selected(),
            AppCommand::ExportSession => {
                if let Err(error) = self.export_session() {
                    self.warnings.push(format!("Export failed: {error}"));
                }
            }
            AppCommand::HalfPageDown => self.half_page_down(),
            AppCommand::HalfPageUp => self.half_page_up(),
            AppCommand::OpenLogs => {
                self.ui.active_panel = ActivePanel::Logs;
                self.ui.log_scroll_offset = self.warnings.len().saturating_sub(self.log_view_rows);
            }
        }
        true
    }

    fn half_page_down(&mut self) {
        if self.ui.active_panel == ActivePanel::StreamDetail {
            let amount = (self.message_view_rows / 2).max(1);
            let count = self.selected_timeline().len();
            for _ in 0..amount {
                self.ui.select_next_message(count, self.message_view_rows);
            }
        } else if self.ui.active_panel == ActivePanel::Logs {
            let amount = (self.log_view_rows / 2).max(1);
            self.ui
                .scroll_logs_down(self.warnings.len(), self.log_view_rows, amount);
        }
    }

    fn half_page_up(&mut self) {
        if self.ui.active_panel == ActivePanel::StreamDetail {
            let amount = (self.message_view_rows / 2).max(1);
            let count = self.selected_timeline().len();
            for _ in 0..amount {
                self.ui
                    .select_previous_message(count, self.message_view_rows);
            }
        } else if self.ui.active_panel == ActivePanel::Logs {
            let amount = (self.log_view_rows / 2).max(1);
            self.ui.scroll_logs_up(amount);
        }
    }

    fn start_new_session(&mut self) {
        self.archives
            .push(self.projections.session().archive_summary(Utc::now()));
        self.archived_timelines.push(ArchivedTimeline {
            session_number: self.projections.session().number,
            started_at: self.projections.session().started_at,
            messages: self.projections.all_messages(),
        });
        let baselines = self.projections.last_seen_baselines();
        let session_number = self.projections.session().number + 1;
        let session = Session::new_numbered("session", session_number, baselines);
        self.projections = ProjectionStore::new(self.catalog.clone(), session, 1000);
        self.ui.reset_message_scroll();
        self.markers.clear();
        self.bookmarks.clear();
    }

    fn bookmark_selected(&mut self) {
        if let Some(message) = self
            .selected_timeline()
            .get(self.ui.selected_message)
            .cloned()
        {
            self.bookmarks.push(MessageBookmark {
                stream: message.stream,
                logical_stream: message.logical_stream,
                shard: message.shard,
                id: message.id,
                message_type: message.message_type,
                at: Utc::now(),
            });
        }
    }

    fn export_session(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.export_dir)?;
        let session_id = self.projections.session().id;
        std::fs::write(
            self.export_dir.join(format!("{session_id}.jsonl")),
            session_jsonl(&self.projections)?,
        )?;
        std::fs::write(
            self.export_dir.join(format!("{session_id}.md")),
            session_markdown_summary(&self.projections)?,
        )?;
        Ok(())
    }
}
