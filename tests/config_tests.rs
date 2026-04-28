use rs_observer::config::AppConfig;

#[test]
fn parses_single_and_sharded_streams() {
    let config = AppConfig::from_yaml(
        r#"
        redis:
          url: redis://127.0.0.1/
        decoders:
          example:
            kind: prost
            message: observer.example.ExampleEvent
            payload_field: payload
        streams:
          - name: example-event-log
            decoder: raw
        sharded_streams:
          - name: example-events
            prefix: example-events-
            start: 0
            end: 3
            width: 2
            decoder: example
        "#,
    )
    .unwrap();

    let catalog = config.catalog().unwrap();

    assert_eq!(config.decoders["example"].payload_field(), Some("payload"));
    assert_eq!(catalog.logical_streams().len(), 2);
    assert_eq!(
        catalog.logical("example-events").unwrap().physical_names(),
        [
            "example-events-00",
            "example-events-01",
            "example-events-02",
            "example-events-03"
        ]
    );
    assert_eq!(
        catalog.physical("example-events-02").unwrap().shard,
        Some(2)
    );
    assert_eq!(
        catalog.physical("example-event-log").unwrap().decoder,
        "raw"
    );
}

#[test]
fn rejects_duplicate_physical_stream_names() {
    let err = AppConfig::from_yaml(
        r#"
        redis:
          url: redis://127.0.0.1/
        streams:
          - name: events-00
            decoder: raw
        sharded_streams:
          - name: events
            prefix: events-
            start: 0
            end: 1
            width: 2
            decoder: raw
        "#,
    )
    .unwrap()
    .catalog()
    .unwrap_err();

    assert!(err.to_string().contains("duplicate physical stream"));
}

#[test]
fn rejects_empty_logical_names() {
    let err = AppConfig::from_yaml(
        r#"
        redis:
          url: redis://127.0.0.1/
        streams:
          - name: ""
            decoder: raw
        "#,
    )
    .unwrap()
    .catalog()
    .unwrap_err();

    assert!(err.to_string().contains("empty stream name"));
}

#[test]
fn parses_checked_in_example_config() {
    let config = AppConfig::from_yaml(include_str!("../examples/observer.yaml")).unwrap();
    let catalog = config.catalog().unwrap();

    assert_eq!(config.redis.initial_urls(), ["redis://127.0.0.1/"]);
    assert_eq!(
        config.decoders["sample_event"].payload_field(),
        Some("payload")
    );
    assert_eq!(
        catalog
            .logical("example-events")
            .unwrap()
            .physical_names()
            .len(),
        16
    );
    assert_eq!(
        catalog.physical("example-event-log").unwrap().decoder,
        "sample_event2"
    );
}

#[test]
fn parses_cluster_redis_nodes() {
    let config = AppConfig::from_yaml(
        r#"
        redis:
          cluster_urls:
            - redis://127.0.0.1:7000/
            - redis://127.0.0.1:7001/
        streams:
          - name: example-event-log
            decoder: raw
        "#,
    )
    .unwrap();

    assert!(config.redis.is_cluster());
    assert_eq!(
        config.redis.initial_urls(),
        ["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/"]
    );
}
