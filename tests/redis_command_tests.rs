use redis::Value;
use rs_observer::redis_client::{
    group_metrics_from_xinfo_groups, last_generated_id_from_xinfo, pending_summary_from_xpending,
    raw_messages_from_xread,
};

#[test]
fn parses_last_generated_id_from_xinfo_stream_response() {
    let value = Value::Bulk(vec![
        Value::Data(b"length".to_vec()),
        Value::Int(2),
        Value::Data(b"last-generated-id".to_vec()),
        Value::Data(b"1745829221244-0".to_vec()),
    ]);

    assert_eq!(
        last_generated_id_from_xinfo(value).unwrap().to_string(),
        "1745829221244-0"
    );
}

#[test]
fn parses_xread_response_into_raw_messages() {
    let value = Value::Bulk(vec![Value::Bulk(vec![
        Value::Data(b"events-00".to_vec()),
        Value::Bulk(vec![Value::Bulk(vec![
            Value::Data(b"1745829221244-0".to_vec()),
            Value::Bulk(vec![
                Value::Data(b"payload".to_vec()),
                Value::Data(b"hello".to_vec()),
                Value::Data(b"type".to_vec()),
                Value::Data(b"Greeting".to_vec()),
            ]),
        ])]),
    ])]);

    let messages = raw_messages_from_xread(value).unwrap();

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].stream, "events-00");
    assert_eq!(messages[0].id.to_string(), "1745829221244-0");
    assert_eq!(messages[0].fields["payload"], b"hello");
    assert_eq!(messages[0].fields["type"], b"Greeting");
}

#[test]
fn parses_xinfo_groups_response() {
    let value = Value::Bulk(vec![Value::Bulk(vec![
        Value::Data(b"name".to_vec()),
        Value::Data(b"worker-group".to_vec()),
        Value::Data(b"consumers".to_vec()),
        Value::Int(3),
        Value::Data(b"pending".to_vec()),
        Value::Int(2),
        Value::Data(b"lag".to_vec()),
        Value::Int(7),
    ])]);

    let groups = group_metrics_from_xinfo_groups("events", value).unwrap();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].stream, "events");
    assert_eq!(groups[0].group, "worker-group");
    assert_eq!(groups[0].consumer_count, 3);
    assert_eq!(groups[0].pending_count, 2);
    assert_eq!(groups[0].lag, Some(7));
}

#[test]
fn parses_xpending_summary_response() {
    let value = Value::Bulk(vec![
        Value::Int(4),
        Value::Data(b"10-0".to_vec()),
        Value::Data(b"20-0".to_vec()),
        Value::Bulk(vec![]),
    ]);

    let pending = pending_summary_from_xpending("events", "worker-group", value).unwrap();

    assert_eq!(pending.stream, "events");
    assert_eq!(pending.group, "worker-group");
    assert_eq!(pending.pending_count, 4);
    assert_eq!(pending.oldest_id.unwrap().to_string(), "10-0");
}
