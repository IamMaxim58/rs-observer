use redis::Value;
use rs_observer::redis_client::{
    group_metrics_from_xinfo_groups, last_generated_id_from_xinfo, pending_summary_from_xpending,
    raw_messages_from_xread, xread_stream_command,
};

#[test]
fn parses_last_generated_id_from_xinfo_stream_response() {
    let value = Value::Array(vec![
        Value::BulkString(b"length".to_vec()),
        Value::Int(2),
        Value::BulkString(b"last-generated-id".to_vec()),
        Value::BulkString(b"1745829221244-0".to_vec()),
    ]);

    assert_eq!(
        last_generated_id_from_xinfo(value).unwrap().to_string(),
        "1745829221244-0"
    );
}

#[test]
fn parses_xread_response_into_raw_messages() {
    let value = Value::Array(vec![Value::Array(vec![
        Value::BulkString(b"events-00".to_vec()),
        Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"1745829221244-0".to_vec()),
            Value::Array(vec![
                Value::BulkString(b"payload".to_vec()),
                Value::BulkString(b"hello".to_vec()),
                Value::BulkString(b"type".to_vec()),
                Value::BulkString(b"Greeting".to_vec()),
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
fn builds_nonblocking_xread_for_one_stream() {
    let command = xread_stream_command("events-00", "10-0".parse().unwrap());
    let packed = String::from_utf8(command.get_packed_command()).unwrap();

    assert!(packed.contains("XREAD"));
    assert!(packed.contains("COUNT"));
    assert!(packed.contains("STREAMS"));
    assert!(packed.contains("events-00"));
    assert!(packed.contains("10-0"));
    assert!(!packed.contains("BLOCK"));
    assert!(!packed.contains("events-01"));
}

#[test]
fn parses_xinfo_groups_response() {
    let value = Value::Array(vec![Value::Array(vec![
        Value::BulkString(b"name".to_vec()),
        Value::BulkString(b"worker-group".to_vec()),
        Value::BulkString(b"consumers".to_vec()),
        Value::Int(3),
        Value::BulkString(b"pending".to_vec()),
        Value::Int(2),
        Value::BulkString(b"lag".to_vec()),
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
    let value = Value::Array(vec![
        Value::Int(4),
        Value::BulkString(b"10-0".to_vec()),
        Value::BulkString(b"20-0".to_vec()),
        Value::Array(vec![]),
    ]);

    let pending = pending_summary_from_xpending("events", "worker-group", value).unwrap();

    assert_eq!(pending.stream, "events");
    assert_eq!(pending.group, "worker-group");
    assert_eq!(pending.pending_count, 4);
    assert_eq!(pending.oldest_id.unwrap().to_string(), "10-0");
}
