use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use prost::Message;
use rs_observer::app::{App, AppCommand, TimelineEntry, TimelineRow, WorkerEvent};
use rs_observer::catalog::{PhysicalStream, StreamCatalog};
use rs_observer::config::{DecoderConfig, ProstDecoderConfig};
use rs_observer::decoder::RawStreamMessage;
use rs_observer::proto_registry::observer::example::ExampleEvent;
use rs_observer::stream_id::StreamId;
use rs_observer::ui::model::ActivePanel;

fn catalog() -> StreamCatalog {
    StreamCatalog::from_physical(vec![
        PhysicalStream {
            name: "events-00".to_string(),
            logical_name: "events".to_string(),
            shard: Some(0),
            decoder: "raw".to_string(),
        },
        PhysicalStream {
            name: "events-01".to_string(),
            logical_name: "events".to_string(),
            shard: Some(1),
            decoder: "raw".to_string(),
        },
    ])
    .unwrap()
}

fn raw(stream: &str, id: &str) -> RawStreamMessage {
    RawStreamMessage {
        stream: stream.to_string(),
        id: id.parse().unwrap(),
        fields: BTreeMap::from([("payload".to_string(), b"hello".to_vec())]),
        observed_at: Utc.timestamp_opt(1, 0).unwrap(),
    }
}

fn raw_with_payload(stream: &str, id: &str, payload: &str) -> RawStreamMessage {
    RawStreamMessage {
        stream: stream.to_string(),
        id: id.parse().unwrap(),
        fields: BTreeMap::from([("payload".to_string(), payload.as_bytes().to_vec())]),
        observed_at: Utc.timestamp_opt(1, 0).unwrap(),
    }
}

#[test]
fn new_session_uses_latest_seen_ids_as_baselines() {
    let mut app = App::with_zero_baselines(catalog());

    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-01", "20-0")));
    app.handle_command(AppCommand::NewSession);
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-1")));

    assert_eq!(app.archives.len(), 1);
    assert_eq!(
        app.projections.session().baseline("events-00"),
        "10-0".parse::<StreamId>().unwrap()
    );
    assert_eq!(
        app.projections.logical_summary("events").unwrap().new_count,
        1
    );
}

#[test]
fn new_session_in_detail_scrolls_to_bottom() {
    let mut app = App::with_zero_baselines(catalog());
    app.message_view_rows = 4;
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-1")));
    app.handle_command(AppCommand::OpenSelected);

    app.handle_command(AppCommand::NewSession);

    assert_eq!(app.ui.selected_message, 1);
    assert_eq!(app.ui.message_scroll_offset, 0);
    let window = app.selected_timeline_window();
    assert!(matches!(
        window.rows.last(),
        Some(TimelineRow::SessionBoundary {
            session_number: 2,
            ..
        })
    ));
}

#[test]
fn detail_timeline_keeps_history_with_new_session_separator() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_command(AppCommand::NewSession);
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-1")));

    let entries = app.selected_timeline_entries();

    assert_eq!(entries.len(), 3);
    assert!(matches!(
        &entries[0],
        TimelineEntry::Message(message) if message.id.to_string() == "10-0"
    ));
    assert!(matches!(
        &entries[1],
        TimelineEntry::SessionBoundary {
            session_number: 2,
            ..
        }
    ));
    assert!(matches!(
        &entries[2],
        TimelineEntry::Message(message) if message.id.to_string() == "10-1"
    ));
    assert_eq!(
        app.projections.logical_summary("events").unwrap().new_count,
        1
    );
}

#[test]
fn new_session_numbers_increment_for_multiple_boundaries() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_command(AppCommand::NewSession);
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-1")));
    app.handle_command(AppCommand::NewSession);

    let boundary_numbers = app
        .selected_timeline_entries()
        .into_iter()
        .filter_map(|entry| match entry {
            TimelineEntry::SessionBoundary { session_number, .. } => Some(session_number),
            TimelineEntry::Message(_) => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(boundary_numbers, [2, 3]);
}

#[test]
fn detail_navigation_skips_new_session_separators() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_command(AppCommand::NewSession);
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-1")));
    app.handle_command(AppCommand::OpenSelected);

    app.handle_command(AppCommand::SelectNext);

    assert_eq!(app.ui.selected_message, 1);
    assert_eq!(app.selected_timeline()[1].id.to_string(), "10-1");
}

#[test]
fn virtual_timeline_window_contains_only_visible_rows() {
    let mut app = App::with_zero_baselines(catalog());
    app.message_view_rows = 7;
    for i in 0..1000 {
        app.handle_worker_event(WorkerEvent::MessageObserved(raw(
            "events-00",
            &format!("{}-0", i + 1),
        )));
    }
    app.handle_command(AppCommand::OpenSelected);
    app.handle_command(AppCommand::JumpBottom);

    let window = app.selected_timeline_window();

    assert!(window.rows.len() <= app.message_view_rows);
    assert_eq!(window.total_messages, 1000);
    assert_eq!(app.ui.selected_message, 999);
}

#[test]
fn search_moves_between_case_insensitive_rendered_text_matches() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::MessageObserved(raw_with_payload(
        "events-00",
        "10-0",
        "client alpha",
    )));
    app.handle_worker_event(WorkerEvent::MessageObserved(raw_with_payload(
        "events-00",
        "10-1",
        "client beta",
    )));
    app.handle_worker_event(WorkerEvent::MessageObserved(raw_with_payload(
        "events-00",
        "10-2",
        "CLIENT alpha again",
    )));
    app.handle_command(AppCommand::OpenSelected);
    app.handle_command(AppCommand::BeginSearchPrompt);
    for ch in "alpha".chars() {
        app.handle_command(AppCommand::PromptChar(ch));
    }
    app.handle_command(AppCommand::SubmitPrompt);

    app.handle_command(AppCommand::SearchNext);
    assert_eq!(app.ui.selected_message, 2);

    app.handle_command(AppCommand::SearchNext);
    assert_eq!(app.ui.selected_message, 0);

    app.handle_command(AppCommand::SearchPrevious);
    assert_eq!(app.ui.selected_message, 2);
}

#[test]
fn filter_removes_non_matching_rows_from_detail_only() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::MessageObserved(raw_with_payload(
        "events-00",
        "10-0",
        "client alpha",
    )));
    app.handle_worker_event(WorkerEvent::MessageObserved(raw_with_payload(
        "events-00",
        "10-1",
        "client beta",
    )));
    app.handle_command(AppCommand::OpenSelected);
    app.handle_command(AppCommand::BeginFilterPrompt);
    for ch in "beta".chars() {
        app.handle_command(AppCommand::PromptChar(ch));
    }
    app.handle_command(AppCommand::SubmitPrompt);

    let visible = app.selected_timeline();

    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id.to_string(), "10-1");
    assert_eq!(
        app.projections.logical_summary("events").unwrap().new_count,
        2
    );
}

#[test]
fn empty_prompt_enter_clears_search_or_filter() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_command(AppCommand::BeginSearchPrompt);
    app.handle_command(AppCommand::PromptChar('x'));
    app.handle_command(AppCommand::SubmitPrompt);
    assert_eq!(app.ui.search_query.as_deref(), Some("x"));

    app.handle_command(AppCommand::BeginSearchPrompt);
    app.handle_command(AppCommand::PromptBackspace);
    app.handle_command(AppCommand::SubmitPrompt);

    assert_eq!(app.ui.search_query, None);
}

#[test]
fn jump_top_and_bottom_work_for_detail_dashboard_and_logs() {
    let mut app = App::with_zero_baselines(catalog());
    app.message_view_rows = 4;
    for i in 0..10 {
        app.handle_worker_event(WorkerEvent::MessageObserved(raw(
            "events-00",
            &format!("{}-0", i + 1),
        )));
    }
    app.handle_command(AppCommand::OpenSelected);
    app.handle_command(AppCommand::JumpBottom);
    assert_eq!(app.ui.selected_message, 9);
    assert_eq!(app.ui.message_scroll_offset, 6);
    app.handle_command(AppCommand::JumpTop);
    assert_eq!(app.ui.selected_message, 0);
    assert_eq!(app.ui.message_scroll_offset, 0);

    app.handle_command(AppCommand::Back);
    app.handle_command(AppCommand::JumpBottom);
    assert_eq!(app.ui.selected_stream, 0);
    app.handle_command(AppCommand::JumpTop);
    assert_eq!(app.ui.selected_stream, 0);

    app.handle_command(AppCommand::OpenLogs);
    app.log_view_rows = 2;
    for i in 0..5 {
        app.handle_worker_event(WorkerEvent::WorkerWarning(format!("warning {i}")));
    }
    app.handle_command(AppCommand::JumpBottom);
    assert_eq!(app.ui.log_scroll_offset, 3);
    app.handle_command(AppCommand::JumpTop);
    assert_eq!(app.ui.log_scroll_offset, 0);
}

#[test]
fn detail_panel_navigation_moves_selected_message() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-01", "11-0")));
    app.handle_command(AppCommand::OpenSelected);

    assert_eq!(app.ui.active_panel, ActivePanel::StreamDetail);
    assert_eq!(app.ui.selected_message, 0);

    app.handle_command(AppCommand::SelectNext);
    assert_eq!(app.ui.selected_message, 1);

    app.handle_command(AppCommand::SelectPrevious);
    assert_eq!(app.ui.selected_message, 0);
}

#[test]
fn detail_panel_navigation_updates_message_scroll_offset_near_edges() {
    let mut app = App::with_zero_baselines(catalog());
    for i in 0..12 {
        app.handle_worker_event(WorkerEvent::MessageObserved(raw(
            "events-00",
            &format!("{}-0", i + 1),
        )));
    }
    app.handle_command(AppCommand::OpenSelected);

    for _ in 0..8 {
        app.handle_command(AppCommand::SelectNext);
    }

    assert_eq!(app.ui.selected_message, 8);
    assert_eq!(app.ui.message_scroll_offset, 1);
}

#[test]
fn detail_panel_ctrl_d_and_ctrl_u_move_by_half_page() {
    let mut app = App::with_zero_baselines(catalog());
    app.message_view_rows = 10;
    for i in 0..30 {
        app.handle_worker_event(WorkerEvent::MessageObserved(raw(
            "events-00",
            &format!("{}-0", i + 1),
        )));
    }
    app.handle_command(AppCommand::OpenSelected);

    app.handle_command(AppCommand::HalfPageDown);
    assert_eq!(app.ui.selected_message, 5);
    assert_eq!(app.ui.message_scroll_offset, 0);

    app.handle_command(AppCommand::HalfPageDown);
    assert_eq!(app.ui.selected_message, 10);
    assert_eq!(app.ui.message_scroll_offset, 3);

    app.handle_command(AppCommand::HalfPageUp);
    assert_eq!(app.ui.selected_message, 5);
    assert_eq!(app.ui.message_scroll_offset, 3);
}

#[test]
fn ctrl_l_opens_logs_screen_and_preserves_warnings() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::WorkerWarning(
        "XINFO GROUPS failed for `example-events-07`: long redis error".to_string(),
    ));

    app.handle_command(AppCommand::OpenLogs);

    assert_eq!(app.ui.active_panel, ActivePanel::Logs);
    assert_eq!(app.warnings.len(), 1);
    assert!(app.warnings[0].contains("example-events-07"));

    app.handle_command(AppCommand::Back);
    assert_eq!(app.ui.active_panel, ActivePanel::Dashboard);
}

#[test]
fn logs_screen_navigation_scrolls_log_lines() {
    let mut app = App::with_zero_baselines(catalog());
    app.log_view_rows = 3;
    for i in 0..10 {
        app.handle_worker_event(WorkerEvent::WorkerWarning(format!("warning {i}")));
    }
    app.handle_command(AppCommand::OpenLogs);

    assert_eq!(app.ui.log_scroll_offset, 7);

    app.handle_command(AppCommand::HalfPageUp);
    assert_eq!(app.ui.log_scroll_offset, 6);

    app.handle_command(AppCommand::SelectPrevious);
    assert_eq!(app.ui.log_scroll_offset, 5);

    app.handle_command(AppCommand::HalfPageDown);
    assert_eq!(app.ui.log_scroll_offset, 6);
}

#[test]
fn bookmarks_and_markers_are_stored_on_current_session() {
    let mut app = App::with_zero_baselines(catalog());
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));
    app.handle_command(AppCommand::OpenSelected);

    app.handle_command(AppCommand::AddMarker);
    app.handle_command(AppCommand::BookmarkSelected);

    assert_eq!(app.markers.len(), 1);
    assert_eq!(app.bookmarks.len(), 1);
    assert_eq!(app.bookmarks[0].stream, "events-00");
    assert_eq!(app.bookmarks[0].id.to_string(), "10-0");
}

#[test]
fn export_command_writes_jsonl_and_markdown_files() {
    let temp = tempfile::tempdir().unwrap();
    let mut app = App::with_zero_baselines(catalog());
    app.export_dir = temp.path().to_path_buf();
    app.handle_worker_event(WorkerEvent::MessageObserved(raw("events-00", "10-0")));

    app.handle_command(AppCommand::ExportSession);

    let jsonl_path = temp
        .path()
        .join(format!("{}.jsonl", app.projections.session().id));
    let markdown_path = temp
        .path()
        .join(format!("{}.md", app.projections.session().id));
    assert!(jsonl_path.exists());
    assert!(markdown_path.exists());
    assert!(std::fs::read_to_string(jsonl_path)
        .unwrap()
        .contains("\"stream\":\"events-00\""));
}

#[test]
fn app_uses_configured_protobuf_decoder_for_stream_messages() {
    let mut app = App::new_with_decoders(
        catalog(),
        BTreeMap::from([
            ("events-00".to_string(), StreamId::ZERO),
            ("events-01".to_string(), StreamId::ZERO),
        ]),
        BTreeMap::from([(
            "raw".to_string(),
            DecoderConfig::Prost(ProstDecoderConfig {
                message: "observer.example.ExampleEvent".to_string(),
                payload_field: "payload".to_string(),
            }),
        )]),
    );
    let payload = ExampleEvent {
        event_type: "ExampleStarted".to_string(),
        event_id: "evt_123".to_string(),
        attempt: 2,
    }
    .encode_to_vec();

    app.handle_worker_event(WorkerEvent::MessageObserved(RawStreamMessage {
        stream: "events-00".to_string(),
        id: "10-0".parse().unwrap(),
        fields: BTreeMap::from([("payload".to_string(), payload)]),
        observed_at: Utc.timestamp_opt(1, 0).unwrap(),
    }));

    let message = app.projections.timeline("events").unwrap().remove(0);
    assert_eq!(message.message_type, "observer.example.ExampleEvent");
    assert_eq!(message.decoded["event_id"], "evt_123");
}
