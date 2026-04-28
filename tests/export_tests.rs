use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use rs_observer::catalog::{PhysicalStream, StreamCatalog};
use rs_observer::decoder::{RawJsonDecoder, RawStreamMessage};
use rs_observer::export::{session_jsonl, session_markdown_summary};
use rs_observer::projection::ProjectionStore;
use rs_observer::session::Session;
use rs_observer::stream_id::StreamId;

fn store_with_message() -> ProjectionStore {
    let catalog = StreamCatalog::from_physical(vec![PhysicalStream {
        name: "example-event-log".to_string(),
        logical_name: "example-event-log".to_string(),
        shard: None,
        decoder: "raw".to_string(),
    }])
    .unwrap();
    let session = Session::new(
        "manual regression",
        BTreeMap::from([("example-event-log".to_string(), StreamId::ZERO)]),
    );
    let mut store = ProjectionStore::new(catalog, session, 100);
    store
        .observe(
            RawStreamMessage {
                stream: "example-event-log".to_string(),
                id: "10-0".parse().unwrap(),
                fields: BTreeMap::from([("payload".to_string(), b"hello".to_vec())]),
                observed_at: Utc.timestamp_opt(1, 0).unwrap(),
            },
            &RawJsonDecoder,
        )
        .unwrap();
    store
}

#[test]
fn jsonl_export_contains_observed_message_fields() {
    let store = store_with_message();

    let jsonl = session_jsonl(&store).unwrap();

    assert!(jsonl.contains("\"stream\":\"example-event-log\""));
    assert!(jsonl.contains("\"stream_id\":\"10-0\""));
    assert!(jsonl.contains("\"payload\":\"hello\""));
}

#[test]
fn markdown_export_contains_session_summary() {
    let store = store_with_message();

    let markdown = session_markdown_summary(&store).unwrap();

    assert!(markdown.contains("# Session: manual regression"));
    assert!(markdown.contains("| example-event-log | 1 |"));
}
