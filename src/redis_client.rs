use std::collections::BTreeMap;

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::{FromRedisValue, Value};

use crate::config::RedisConfig;
use crate::decoder::RawStreamMessage;
use crate::projection::{GroupMetric, PendingSummary};
use crate::stream_id::StreamId;

pub struct RedisObserverClient {
    connection: ConnectionManager,
}

impl RedisObserverClient {
    pub async fn connect(config: &RedisConfig) -> Result<Self> {
        let client = redis::Client::open(config.url.as_str()).context("invalid Redis URL")?;
        let connection = client
            .get_connection_manager()
            .await
            .context("failed to connect to Redis")?;
        Ok(Self { connection })
    }

    pub async fn xinfo_stream_last_id(&mut self, stream: &str) -> Result<StreamId> {
        let value: Value = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(stream)
            .query_async(&mut self.connection)
            .await
            .with_context(|| format!("failed to read XINFO STREAM for `{stream}`"))?;
        last_generated_id_from_xinfo(value)
    }

    pub async fn xrange_after(
        &mut self,
        stream: &str,
        baseline: StreamId,
        count: usize,
    ) -> Result<Vec<RawStreamMessage>> {
        let start = format!("({baseline}");
        let entries: Vec<(String, BTreeMap<String, Vec<u8>>)> = redis::cmd("XRANGE")
            .arg(stream)
            .arg(start)
            .arg("+")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut self.connection)
            .await
            .with_context(|| format!("failed to read XRANGE for `{stream}`"))?;
        entries
            .into_iter()
            .map(|(id, fields)| {
                Ok(RawStreamMessage {
                    stream: stream.to_string(),
                    id: id.parse()?,
                    fields,
                    observed_at: chrono::Utc::now(),
                })
            })
            .collect()
    }

    pub async fn xread_block(
        &mut self,
        stream_positions: &BTreeMap<String, StreamId>,
        timeout_ms: usize,
    ) -> Result<Vec<RawStreamMessage>> {
        if stream_positions.is_empty() {
            return Ok(Vec::new());
        }

        let mut cmd = redis::cmd("XREAD");
        cmd.arg("BLOCK").arg(timeout_ms).arg("STREAMS");
        for stream in stream_positions.keys() {
            cmd.arg(stream);
        }
        for id in stream_positions.values() {
            cmd.arg(id.to_string());
        }

        let value: Value = cmd
            .query_async(&mut self.connection)
            .await
            .context("failed to read XREAD")?;
        raw_messages_from_xread(value)
    }

    pub async fn xinfo_groups(&mut self, stream: &str) -> Result<Vec<GroupMetric>> {
        let value: Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(stream)
            .query_async(&mut self.connection)
            .await
            .with_context(|| format!("failed to read XINFO GROUPS for `{stream}`"))?;
        group_metrics_from_xinfo_groups(stream, value)
    }

    pub async fn xpending_summary(&mut self, stream: &str, group: &str) -> Result<PendingSummary> {
        let value: Value = redis::cmd("XPENDING")
            .arg(stream)
            .arg(group)
            .query_async(&mut self.connection)
            .await
            .with_context(|| format!("failed to read XPENDING for `{stream}` / `{group}`"))?;
        pending_summary_from_xpending(stream, group, value)
    }
}

pub fn last_generated_id_from_xinfo(value: Value) -> Result<StreamId> {
    let items: Vec<Value> = FromRedisValue::from_redis_value(&value)?;
    for pair in items.chunks(2) {
        if pair.len() != 2 {
            continue;
        }
        let key: String = FromRedisValue::from_redis_value(&pair[0])?;
        if key == "last-generated-id" {
            let id: String = FromRedisValue::from_redis_value(&pair[1])?;
            return Ok(id.parse()?);
        }
    }
    Ok(StreamId::ZERO)
}

pub fn raw_messages_from_xread(value: Value) -> Result<Vec<RawStreamMessage>> {
    let mut messages = Vec::new();
    let Value::Bulk(streams) = value else {
        return Ok(messages);
    };

    for stream_value in streams {
        let Value::Bulk(stream_tuple) = stream_value else {
            continue;
        };
        if stream_tuple.len() != 2 {
            continue;
        }
        let stream_name = redis_string(&stream_tuple[0])?;
        let Value::Bulk(entries) = &stream_tuple[1] else {
            continue;
        };

        for entry_value in entries {
            let Value::Bulk(entry_tuple) = entry_value else {
                continue;
            };
            if entry_tuple.len() != 2 {
                continue;
            }
            let id = redis_string(&entry_tuple[0])?.parse()?;
            let Value::Bulk(field_values) = &entry_tuple[1] else {
                continue;
            };
            let mut fields = BTreeMap::new();
            for pair in field_values.chunks(2) {
                if pair.len() != 2 {
                    continue;
                }
                fields.insert(redis_string(&pair[0])?, redis_bytes(&pair[1])?);
            }
            messages.push(RawStreamMessage {
                stream: stream_name.clone(),
                id,
                fields,
                observed_at: chrono::Utc::now(),
            });
        }
    }

    Ok(messages)
}

pub fn group_metrics_from_xinfo_groups(stream: &str, value: Value) -> Result<Vec<GroupMetric>> {
    let mut groups = Vec::new();
    let Value::Bulk(group_values) = value else {
        return Ok(groups);
    };

    for group_value in group_values {
        let Value::Bulk(fields) = group_value else {
            continue;
        };
        let mut name = None;
        let mut consumers = 0;
        let mut pending = 0;
        let mut lag = None;

        for pair in fields.chunks(2) {
            if pair.len() != 2 {
                continue;
            }
            let key = redis_string(&pair[0])?;
            match key.as_str() {
                "name" => name = Some(redis_string(&pair[1])?),
                "consumers" => consumers = redis_u64(&pair[1])?,
                "pending" => pending = redis_u64(&pair[1])?,
                "lag" => lag = Some(redis_i64(&pair[1])?),
                _ => {}
            }
        }

        if let Some(group) = name {
            groups.push(GroupMetric {
                stream: stream.to_string(),
                group,
                consumer_count: consumers,
                pending_count: pending,
                lag,
            });
        }
    }

    Ok(groups)
}

pub fn pending_summary_from_xpending(
    stream: &str,
    group: &str,
    value: Value,
) -> Result<PendingSummary> {
    let Value::Bulk(values) = value else {
        return Ok(PendingSummary {
            stream: stream.to_string(),
            group: group.to_string(),
            pending_count: 0,
            oldest_id: None,
        });
    };

    let pending_count = values.first().map(redis_u64).transpose()?.unwrap_or(0);
    let oldest_id = values
        .get(1)
        .and_then(|value| match value {
            Value::Nil => None,
            _ => Some(redis_string(value)),
        })
        .transpose()?
        .map(|id| id.parse())
        .transpose()?;

    Ok(PendingSummary {
        stream: stream.to_string(),
        group: group.to_string(),
        pending_count,
        oldest_id,
    })
}

fn redis_string(value: &Value) -> Result<String> {
    match value {
        Value::Data(bytes) => Ok(String::from_utf8(bytes.clone())?),
        Value::Status(text) => Ok(text.clone()),
        Value::Okay => Ok("OK".to_string()),
        Value::Int(number) => Ok(number.to_string()),
        Value::Nil | Value::Bulk(_) => anyhow::bail!("expected Redis string value"),
    }
}

fn redis_bytes(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Data(bytes) => Ok(bytes.clone()),
        Value::Status(text) => Ok(text.as_bytes().to_vec()),
        Value::Okay => Ok(b"OK".to_vec()),
        Value::Int(number) => Ok(number.to_string().into_bytes()),
        Value::Nil | Value::Bulk(_) => anyhow::bail!("expected Redis bytes value"),
    }
}

fn redis_i64(value: &Value) -> Result<i64> {
    match value {
        Value::Int(number) => Ok(*number),
        Value::Data(bytes) => Ok(std::str::from_utf8(bytes)?.parse()?),
        Value::Status(text) => Ok(text.parse()?),
        Value::Okay | Value::Nil | Value::Bulk(_) => anyhow::bail!("expected Redis integer value"),
    }
}

fn redis_u64(value: &Value) -> Result<u64> {
    let number = redis_i64(value)?;
    if number < 0 {
        anyhow::bail!("expected non-negative Redis integer value");
    }
    Ok(number as u64)
}
