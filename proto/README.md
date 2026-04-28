# Protobuf Decoder Notes

The Protobuf integration uses `prost_build` from `build.rs`. A local `protoc`
installation is required.

Configured decoder IDs map streams to a payload field and a compiled Rust message
type:

```yaml
decoders:
  sample_event:
    kind: prost
    message: observer.example.ExampleEvent
    payload_field: payload
```

Streams opt into that decoder by ID:

```yaml
streams:
  - name: example-events
    decoder: sample_event
```

The deliberate customization point is `src/proto_registry.rs`. Add generated
modules and message decode registrations there when bringing real project
schemas into this repo.

Currently supported example messages:

```proto
syntax = "proto3";

package observer.example;

message ExampleEvent {
  string event_type = 1;
  string event_id = 2;
  uint32 attempt = 3;
}

message ExampleEvent2 {
  string event_type = 1;
  string event_id = 2;
  bool success = 3;
}
```

Unknown decoder IDs fall back to raw JSON rendering.
