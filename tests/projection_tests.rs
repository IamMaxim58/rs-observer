use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{TimeZone, Utc};
use rs_observer::catalog::{PhysicalStream, StreamCatalog};
use rs_observer::decoder::{RawJsonDecoder, RawStreamMessage};
use rs_observer::projection::{GroupMetric, PendingSummary, ProjectionStore, StreamStatus};
use rs_observer::session::Session;
use rs_observer::stream_id::StreamId;

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

fn four_shard_catalog() -> StreamCatalog {
    StreamCatalog::from_physical(
        (0..4)
            .map(|shard| PhysicalStream {
                name: format!("events-{shard:02}"),
                logical_name: "events".to_string(),
                shard: Some(shard),
                decoder: "raw".to_string(),
            })
            .collect(),
    )
    .unwrap()
}

fn raw(stream: &str, id: &str, observed_second: i64) -> RawStreamMessage {
    let mut fields = BTreeMap::new();
    fields.insert("payload".to_string(), b"hello".to_vec());
    RawStreamMessage {
        stream: stream.to_string(),
        id: StreamId::from_str(id).unwrap(),
        fields,
        observed_at: Utc.timestamp_opt(observed_second, 0).unwrap(),
    }
}

#[test]
fn session_excludes_messages_at_or_before_baseline() {
    let mut baselines = BTreeMap::new();
    baselines.insert("events-00".to_string(), StreamId::from_str("10-0").unwrap());
    let session = Session::new("test", baselines);

    assert!(!session.includes("events-00", StreamId::from_str("9-9").unwrap()));
    assert!(!session.includes("events-00", StreamId::from_str("10-0").unwrap()));
    assert!(session.includes("events-00", StreamId::from_str("10-1").unwrap()));
}

#[test]
fn projection_ignores_pre_baseline_messages_and_counts_new_messages() {
    let catalog = catalog();
    let session = Session::new(
        "test",
        BTreeMap::from([
            ("events-00".to_string(), StreamId::from_str("10-0").unwrap()),
            ("events-01".to_string(), StreamId::from_str("10-0").unwrap()),
        ]),
    );
    let mut store = ProjectionStore::new(catalog, session, 100);
    let decoder = RawJsonDecoder;

    store
        .observe(raw("events-00", "10-0", 1), &decoder)
        .unwrap();
    store
        .observe(raw("events-00", "10-1", 2), &decoder)
        .unwrap();

    let summary = store.logical_summary("events").unwrap();
    assert_eq!(summary.new_count, 1);
    assert_eq!(summary.status, StreamStatus::Ok);
}

#[test]
fn logical_timeline_merges_shards_by_stream_id() {
    let catalog = catalog();
    let session = Session::new(
        "test",
        BTreeMap::from([
            ("events-00".to_string(), StreamId::ZERO),
            ("events-01".to_string(), StreamId::ZERO),
        ]),
    );
    let mut store = ProjectionStore::new(catalog, session, 100);
    let decoder = RawJsonDecoder;

    store
        .observe(raw("events-01", "12-0", 3), &decoder)
        .unwrap();
    store
        .observe(raw("events-00", "11-0", 2), &decoder)
        .unwrap();

    let ids: Vec<_> = store
        .timeline("events")
        .unwrap()
        .into_iter()
        .map(|message| message.id.to_string())
        .collect();

    assert_eq!(ids, ["11-0", "12-0"]);
}

#[test]
fn detects_hot_shard_when_one_shard_dominates() {
    let catalog = four_shard_catalog();
    let session = Session::new(
        "test",
        BTreeMap::from([
            ("events-00".to_string(), StreamId::ZERO),
            ("events-01".to_string(), StreamId::ZERO),
            ("events-02".to_string(), StreamId::ZERO),
            ("events-03".to_string(), StreamId::ZERO),
        ]),
    );
    let mut store = ProjectionStore::new(catalog, session, 100);
    let decoder = RawJsonDecoder;

    for i in 1..=10 {
        store
            .observe(raw("events-00", &format!("{i}-0"), i), &decoder)
            .unwrap();
    }
    store
        .observe(raw("events-01", "11-0", 11), &decoder)
        .unwrap();

    assert_eq!(
        store.logical_summary("events").unwrap().status,
        StreamStatus::HotShard
    );
}

#[test]
fn aggregates_consumer_group_lag_and_pending_counts() {
    let catalog = catalog();
    let session = Session::new(
        "test",
        BTreeMap::from([
            ("events-00".to_string(), StreamId::ZERO),
            ("events-01".to_string(), StreamId::ZERO),
        ]),
    );
    let mut store = ProjectionStore::new(catalog, session, 100);

    store.update_group_metric(GroupMetric {
        stream: "events-00".to_string(),
        group: "workers".to_string(),
        consumer_count: 2,
        pending_count: 3,
        lag: Some(5),
    });
    store.update_pending_summary(PendingSummary {
        stream: "events-01".to_string(),
        group: "workers".to_string(),
        pending_count: 4,
        oldest_id: Some("9-0".parse().unwrap()),
    });

    let summary = store.logical_summary("events").unwrap();
    assert_eq!(summary.group_lag, Some(5));
    assert_eq!(summary.pending_count, 7);
}
