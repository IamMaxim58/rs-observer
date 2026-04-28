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
use crate::ui::model::{selected_logical_name, ActivePanel, PromptKind, UiState};

#[derive(Debug, Clone, PartialEq)]
pub enum TimelineEntry {
    Message(DecodedMessage),
    SessionBoundary {
        session_number: u64,
        started_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimelineRow {
    Message {
        message: DecodedMessage,
        search_match: bool,
    },
    SessionBoundary {
        session_number: u64,
        started_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimelineWindow {
    pub rows: Vec<TimelineRow>,
    pub selected_message: Option<DecodedMessage>,
    pub total_messages: usize,
    pub visible_messages: usize,
    pub match_count: usize,
    pub search_query: Option<String>,
    pub filter_query: Option<String>,
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
    BeginSearchPrompt,
    BeginFilterPrompt,
    PromptChar(char),
    PromptBackspace,
    SubmitPrompt,
    CancelPrompt,
    SearchNext,
    SearchPrevious,
    JumpTop,
    JumpBottom,
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
        self.selected_filtered_messages()
    }

    pub fn selected_timeline_entries(&self) -> Vec<TimelineEntry> {
        self.selected_timeline_entries_for_filter(true)
    }

    pub fn selected_timeline_window(&self) -> TimelineWindow {
        let entries = self.selected_timeline_entries();
        let total_messages = self.selected_unfiltered_messages().len();
        let visible_messages = entries
            .iter()
            .filter(|entry| matches!(entry, TimelineEntry::Message(_)))
            .count();
        let match_count = entries
            .iter()
            .filter(|entry| match entry {
                TimelineEntry::Message(message) => self.message_matches_search(message),
                TimelineEntry::SessionBoundary { .. } => false,
            })
            .count();
        let selected_message = entries
            .iter()
            .filter_map(|entry| match entry {
                TimelineEntry::Message(message) => Some(message.clone()),
                TimelineEntry::SessionBoundary { .. } => None,
            })
            .nth(self.ui.selected_message);
        let rows = self.timeline_rows_for_window(&entries);

        TimelineWindow {
            rows,
            selected_message,
            total_messages,
            visible_messages,
            match_count,
            search_query: self.ui.search_query.clone(),
            filter_query: self.ui.filter_query.clone(),
        }
    }

    fn selected_timeline_entries_for_filter(&self, apply_filter: bool) -> Vec<TimelineEntry> {
        let summaries = self.summaries();
        let Some(logical_name) = selected_logical_name(&summaries, &self.ui) else {
            return Vec::new();
        };

        let mut entries = Vec::new();
        for archived in &self.archived_timelines {
            let messages = archived
                .messages
                .iter()
                .filter(|message| message.logical_stream == logical_name)
                .filter(|message| !apply_filter || self.message_matches_filter(message))
                .cloned()
                .collect::<Vec<_>>();
            if archived.session_number > 1
                && (!messages.is_empty() || self.ui.filter_query.is_none())
            {
                entries.push(TimelineEntry::SessionBoundary {
                    session_number: archived.session_number,
                    started_at: archived.started_at,
                });
            }
            entries.extend(messages.into_iter().map(TimelineEntry::Message));
        }

        let current_session = self.projections.session();
        let current = self
            .projections
            .timeline(&logical_name)
            .unwrap_or_default()
            .into_iter()
            .filter(|message| !apply_filter || self.message_matches_filter(message))
            .collect::<Vec<_>>();
        if current_session.number > 1 && (!current.is_empty() || self.ui.filter_query.is_none()) {
            entries.push(TimelineEntry::SessionBoundary {
                session_number: current_session.number,
                started_at: current_session.started_at,
            });
        }
        entries.extend(current.into_iter().map(TimelineEntry::Message));

        entries
    }

    fn selected_unfiltered_messages(&self) -> Vec<DecodedMessage> {
        self.selected_timeline_entries_for_filter(false)
            .into_iter()
            .filter_map(|entry| match entry {
                TimelineEntry::Message(message) => Some(message),
                TimelineEntry::SessionBoundary { .. } => None,
            })
            .collect()
    }

    fn selected_filtered_messages(&self) -> Vec<DecodedMessage> {
        self.selected_timeline_entries_for_filter(true)
            .into_iter()
            .filter_map(|entry| match entry {
                TimelineEntry::Message(message) => Some(message),
                TimelineEntry::SessionBoundary { .. } => None,
            })
            .collect()
    }

    fn timeline_rows_for_window(&self, entries: &[TimelineEntry]) -> Vec<TimelineRow> {
        let mut rows = Vec::new();
        let mut skipped_messages = 0;
        for entry in entries {
            match entry {
                TimelineEntry::Message(_) if skipped_messages < self.ui.message_scroll_offset => {
                    skipped_messages += 1;
                }
                TimelineEntry::Message(message) => {
                    if rows.len() >= self.message_view_rows {
                        break;
                    }
                    rows.push(TimelineRow::Message {
                        message: message.clone(),
                        search_match: self.message_matches_search(message),
                    });
                }
                TimelineEntry::SessionBoundary {
                    session_number,
                    started_at,
                } if skipped_messages >= self.ui.message_scroll_offset => {
                    if rows.len() >= self.message_view_rows {
                        break;
                    }
                    rows.push(TimelineRow::SessionBoundary {
                        session_number: *session_number,
                        started_at: *started_at,
                    });
                }
                TimelineEntry::SessionBoundary { .. } => {}
            }
        }
        rows
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
            AppCommand::BeginSearchPrompt => self.ui.begin_search_prompt(),
            AppCommand::BeginFilterPrompt => self.ui.begin_filter_prompt(),
            AppCommand::PromptChar(ch) => self.ui.prompt_char(ch),
            AppCommand::PromptBackspace => self.ui.prompt_backspace(),
            AppCommand::SubmitPrompt => self.submit_prompt(),
            AppCommand::CancelPrompt => self.ui.cancel_prompt(),
            AppCommand::SearchNext => self.select_search_match(true),
            AppCommand::SearchPrevious => self.select_search_match(false),
            AppCommand::JumpTop => self.jump_top(),
            AppCommand::JumpBottom => self.jump_bottom(),
        }
        true
    }

    fn submit_prompt(&mut self) {
        let Some(prompt) = self.ui.submit_prompt() else {
            return;
        };
        let value = (!prompt.draft.is_empty()).then_some(prompt.draft);
        match prompt.kind {
            PromptKind::Search => self.ui.search_query = value,
            PromptKind::Filter => {
                self.ui.filter_query = value;
                self.ui.selected_message = 0;
                self.ui.message_scroll_offset = 0;
            }
        }
        let count = self.selected_timeline().len();
        if count == 0 {
            self.ui.reset_message_scroll();
        } else if self.ui.selected_message >= count {
            self.ui.selected_message = count - 1;
            self.ui.adjust_message_scroll(count, self.message_view_rows);
        }
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
        if self.ui.active_panel == ActivePanel::StreamDetail {
            self.jump_bottom();
        } else {
            self.ui.reset_message_scroll();
        }
        self.markers.clear();
        self.bookmarks.clear();
    }

    fn jump_top(&mut self) {
        match self.ui.active_panel {
            ActivePanel::Dashboard => self.ui.selected_stream = 0,
            ActivePanel::StreamDetail => self.ui.reset_message_scroll(),
            ActivePanel::Logs => self.ui.log_scroll_offset = 0,
        }
    }

    fn jump_bottom(&mut self) {
        match self.ui.active_panel {
            ActivePanel::Dashboard => {
                self.ui.selected_stream = self.summaries().len().saturating_sub(1);
            }
            ActivePanel::StreamDetail => {
                let count = self.selected_timeline().len();
                if count == 0 {
                    self.ui.reset_message_scroll();
                } else {
                    self.ui.selected_message = count - 1;
                    self.ui.message_scroll_offset = count.saturating_sub(self.message_view_rows);
                }
            }
            ActivePanel::Logs => {
                self.ui.log_scroll_offset = self.warnings.len().saturating_sub(self.log_view_rows);
            }
        }
    }

    fn select_search_match(&mut self, forward: bool) {
        if self.ui.active_panel != ActivePanel::StreamDetail || self.ui.search_query.is_none() {
            return;
        }
        let messages = self.selected_timeline();
        if messages.is_empty() {
            return;
        }
        let mut indexes = messages
            .iter()
            .enumerate()
            .filter_map(|(index, message)| self.message_matches_search(message).then_some(index))
            .collect::<Vec<_>>();
        if indexes.is_empty() {
            return;
        }
        indexes.sort_unstable();
        let selected = self.ui.selected_message;
        let next = if forward {
            indexes
                .iter()
                .copied()
                .find(|index| *index > selected)
                .unwrap_or(indexes[0])
        } else {
            indexes
                .iter()
                .rev()
                .copied()
                .find(|index| *index < selected)
                .unwrap_or(*indexes.last().unwrap())
        };
        self.ui.selected_message = next;
        self.ui
            .adjust_message_scroll(messages.len(), self.message_view_rows);
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

    fn message_matches_filter(&self, message: &DecodedMessage) -> bool {
        self.ui
            .filter_query
            .as_ref()
            .is_none_or(|query| message_matches_query(message, query))
    }

    fn message_matches_search(&self, message: &DecodedMessage) -> bool {
        self.ui
            .search_query
            .as_ref()
            .is_some_and(|query| message_matches_query(message, query))
    }
}

fn message_matches_query(message: &DecodedMessage, query: &str) -> bool {
    message_render_text(message).contains(&query.to_lowercase())
}

fn message_render_text(message: &DecodedMessage) -> String {
    let shard = message
        .shard
        .map(|shard| format!("{shard:02}"))
        .unwrap_or_else(|| "-".to_string());
    format!(
        "{} {} {} {} {} {}",
        message.stream,
        shard,
        message.id,
        message.message_type,
        message.decoded,
        message.decode_error.as_deref().unwrap_or("")
    )
    .to_lowercase()
}
