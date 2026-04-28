use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};

pub mod observer {
    pub mod example {
        include!(concat!(env!("OUT_DIR"), "/observer.example.rs"));
    }
}

pub fn decode_message(message_name: &str, payload: &[u8]) -> Result<Value> {
    match message_name {
        "observer.example.ExampleEvent" => {
            let decoded = observer::example::ExampleEvent::decode(payload)?;
            Ok(json!({
                "event_type": decoded.event_type,
                "event_id": decoded.event_id,
                "attempt": decoded.attempt,
            }))
        }
        "observer.example.ExampleEvent2" => {
            let decoded = observer::example::ExampleEvent2::decode(payload)?;
            Ok(json!({
                "event_type": decoded.event_type,
                "event_id": decoded.event_id,
                "success": decoded.success,
            }))
        }
        other => anyhow::bail!("unsupported protobuf message `{other}`"),
    }
}
