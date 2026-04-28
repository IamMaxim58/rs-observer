use anyhow::Result;
use serde_json::json;

use crate::projection::ProjectionStore;

pub fn session_jsonl(store: &ProjectionStore) -> Result<String> {
    let mut lines = Vec::new();
    for message in store.all_messages() {
        lines.push(serde_json::to_string(&json!({
            "session_id": store.session().id.to_string(),
            "session_name": store.session().name,
            "stream": message.stream,
            "logical_stream": message.logical_stream,
            "shard": message.shard,
            "stream_id": message.id.to_string(),
            "observed_at": message.observed_at.to_rfc3339(),
            "message_type": message.message_type,
            "decoded": message.decoded,
            "raw_fields": render_raw_fields(&message.raw_fields),
        }))?);
    }
    Ok(lines.join("\n"))
}

pub fn session_markdown_summary(store: &ProjectionStore) -> Result<String> {
    let mut markdown = format!("# Session: {}\n\n", store.session().name);
    markdown.push_str(&format!(
        "Started: {}\n\n",
        store.session().started_at.to_rfc3339()
    ));
    markdown.push_str("| Stream | Messages | Decode errors | Status |\n");
    markdown.push_str("| --- | ---: | ---: | --- |\n");
    for summary in store.logical_summaries() {
        markdown.push_str(&format!(
            "| {} | {} | {} | {:?} |\n",
            summary.name, summary.new_count, summary.decode_errors, summary.status
        ));
    }
    Ok(markdown)
}

fn render_raw_fields(fields: &std::collections::BTreeMap<String, Vec<u8>>) -> serde_json::Value {
    let mut rendered = serde_json::Map::new();
    for (key, value) in fields {
        match std::str::from_utf8(value) {
            Ok(text) => {
                rendered.insert(key.clone(), serde_json::Value::String(text.to_string()));
            }
            Err(_) => {
                rendered.insert(
                    key.clone(),
                    json!({
                        "hex": hex::encode(value),
                    }),
                );
            }
        }
    }
    serde_json::Value::Object(rendered)
}
