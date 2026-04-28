use rs_observer::config::AppConfig;
use rs_observer::producer::ProducerMessageFactory;

#[test]
fn producer_builds_messages_for_configured_example_streams() {
    let config = AppConfig::from_yaml(
        r#"
        redis:
          url: redis://127.0.0.1/
        decoders:
          example:
            kind: prost
            message: observer.example.ExampleEvent
            payload_field: payload
          example2:
            kind: prost
            message: observer.example.ExampleEvent2
            payload_field: payload
        streams:
          - name: example-event-log
            decoder: example2
        sharded_streams:
          - name: example-events
            prefix: example-events-
            start: 0
            end: 1
            width: 2
            decoder: example
        "#,
    )
    .unwrap();
    let catalog = config.catalog().unwrap();
    let mut factory = ProducerMessageFactory::new(&config, &catalog);

    let first = factory.next_message().unwrap();
    let second = factory.next_message().unwrap();
    let third = factory.next_message().unwrap();

    assert_eq!(first.stream, "example-event-log");
    assert_eq!(first.fields["type"], b"observer.example.ExampleEvent2");
    assert!(first.fields["payload"].len() > 4);
    assert_eq!(second.stream, "example-events-00");
    assert_eq!(second.fields["type"], b"observer.example.ExampleEvent");
    assert_eq!(third.stream, "example-events-01");
    assert_eq!(third.fields["type"], b"observer.example.ExampleEvent");
}
