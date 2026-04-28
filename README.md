# rs-observer

`rs-observer` is a Rust terminal utility for passive observation of Redis Streams during manual testing.

The MVP focuses on observe-only behavior:

- read configured single and sharded streams;
- record a session baseline without modifying Redis;
- show session-scoped message counts and timelines;
- inspect raw decoded Redis stream fields.
- decode configured Protobuf payloads generated with `prost_build`.

Write actions such as `XADD`, `XACK`, `XDEL`, `XTRIM`, `XGROUP`, `XCLAIM`, and `XAUTOCLAIM` are intentionally out of scope for the initial tool.

Planned startup shape:

```bash
cargo run -- --config examples/observer.yaml
```

The repository expects `protoc` to be installed. Example schemas live under
`proto/`, are compiled by `build.rs`, and are registered in `src/proto_registry.rs`.

To generate manual test traffic:

```bash
cargo run --example producer -- --config examples/observer.yaml
```
