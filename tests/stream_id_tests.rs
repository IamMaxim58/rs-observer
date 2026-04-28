use std::str::FromStr;

use rs_observer::stream_id::StreamId;

#[test]
fn parses_displays_and_orders_stream_ids() {
    let first = StreamId::from_str("1745829221244-0").unwrap();
    let second = StreamId::from_str("1745829221244-1").unwrap();
    let third = StreamId::from_str("1745829221245-0").unwrap();

    assert_eq!(first.to_string(), "1745829221244-0");
    assert!(first < second);
    assert!(second < third);
}

#[test]
fn rejects_invalid_stream_ids() {
    assert!(StreamId::from_str("").is_err());
    assert!(StreamId::from_str("1").is_err());
    assert!(StreamId::from_str("1-").is_err());
    assert!(StreamId::from_str("abc-0").is_err());
    assert!(StreamId::from_str("1-abc").is_err());
}

#[test]
fn compares_against_zero_baseline() {
    assert_eq!(StreamId::ZERO.to_string(), "0-0");
    assert!(StreamId::from_str("1-0").unwrap().is_after(StreamId::ZERO));
    assert!(!StreamId::ZERO.is_after(StreamId::ZERO));
}
