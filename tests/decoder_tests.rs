use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use prost::Message;
use rs_observer::config::{DecoderConfig, ProstDecoderConfig};
use rs_observer::decoder::{DecoderRegistry, RawStreamMessage, StaticProstDecoderRegistry};
use rs_observer::proto_registry::observer::example::{ExampleEvent, ExampleEvent2};
use rs_observer::stream_id::StreamId;

fn raw_message(fields: BTreeMap<String, Vec<u8>>) -> RawStreamMessage {
    RawStreamMessage {
        stream: "events".to_string(),
        id: StreamId::ZERO,
        fields,
        observed_at: Utc.timestamp_opt(1, 0).unwrap(),
    }
}

#[test]
fn static_prost_registry_decodes_example_event_payload() {
    let payload = ExampleEvent {
        event_type: "ExampleStarted".to_string(),
        event_id: "evt_123".to_string(),
        attempt: 2,
    }
    .encode_to_vec();
    let message = raw_message(BTreeMap::from([("payload".to_string(), payload)]));
    let registry = StaticProstDecoderRegistry::from_configs(BTreeMap::from([(
        "example".to_string(),
        DecoderConfig::Prost(ProstDecoderConfig {
            message: "observer.example.ExampleEvent".to_string(),
            payload_field: "payload".to_string(),
        }),
    )]));

    let decoded = registry.decode("example", &message).unwrap();

    assert_eq!(decoded.0, "observer.example.ExampleEvent");
    assert_eq!(decoded.1["event_type"], "ExampleStarted");
    assert_eq!(decoded.1["event_id"], "evt_123");
    assert_eq!(decoded.1["attempt"], 2);
}

#[test]
fn static_prost_registry_decodes_example_event2_payload() {
    let payload = ExampleEvent2 {
        event_type: "ExampleCompleted".to_string(),
        event_id: "evt_456".to_string(),
        success: true,
    }
    .encode_to_vec();
    let message = raw_message(BTreeMap::from([("payload".to_string(), payload)]));
    let registry = StaticProstDecoderRegistry::from_configs(BTreeMap::from([(
        "example2".to_string(),
        DecoderConfig::Prost(ProstDecoderConfig {
            message: "observer.example.ExampleEvent2".to_string(),
            payload_field: "payload".to_string(),
        }),
    )]));

    let decoded = registry.decode("example2", &message).unwrap();

    assert_eq!(decoded.0, "observer.example.ExampleEvent2");
    assert_eq!(decoded.1["event_type"], "ExampleCompleted");
    assert_eq!(decoded.1["event_id"], "evt_456");
    assert_eq!(decoded.1["success"], true);
}

#[test]
fn unknown_decoder_id_falls_back_to_raw_json() {
    let message = raw_message(BTreeMap::from([(
        "payload".to_string(),
        b"not protobuf".to_vec(),
    )]));
    let registry = StaticProstDecoderRegistry::from_configs(BTreeMap::new());

    let decoded = registry.decode("missing", &message).unwrap();

    assert_eq!(decoded.0, "raw");
    assert_eq!(decoded.1["payload"], "not protobuf");
}

#[test]
fn prost_decoder_reports_missing_payload_field() {
    let message = raw_message(BTreeMap::new());
    let registry = StaticProstDecoderRegistry::from_configs(BTreeMap::from([(
        "example".to_string(),
        DecoderConfig::Prost(ProstDecoderConfig {
            message: "observer.example.ExampleEvent".to_string(),
            payload_field: "payload".to_string(),
        }),
    )]));

    let err = registry.decode("example", &message).unwrap_err();

    assert!(err.to_string().contains("payload field `payload`"));
}
