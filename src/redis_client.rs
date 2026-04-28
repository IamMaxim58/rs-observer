use std::collections::BTreeMap;

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{FromRedisValue, Value};

use crate::config::RedisConfig;
use crate::decoder::RawStreamMessage;
use crate::projection::{GroupMetric, PendingSummary};
use crate::stream_id::StreamId;

pub struct RedisObserverClient {
    connection: RedisConnection,
}

enum RedisConnection {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl RedisConnection {
    async fn query<T: FromRedisValue>(&mut self, cmd: &mut redis::Cmd) -> redis::RedisResult<T> {
        match self {
            Self::Single(connection) => cmd.query_async(connection).await,
            Self::Cluster(connection) => cmd.query_async(connection).await,
        }
    }
}

impl RedisObserverClient {
    pub async fn connect(config: &RedisConfig) -> Result<Self> {
        let connection = if config.is_cluster() {
            let client =
                ClusterClient::new(config.initial_urls()).context("invalid Redis Cluster URLs")?;
            RedisConnection::Cluster(
                client
                    .get_async_connection()
                    .await
                    .context("failed to connect to Redis Cluster")?,
            )
        } else {
            let client = redis::Client::open(config.single_url()?).context("invalid Redis URL")?;
            RedisConnection::Single(
                client
                    .get_connection_manager()
                    .await
                    .context("failed to connect to Redis")?,
            )
        };
        Ok(Self { connection })
    }

    pub async fn xinfo_stream_last_id(&mut self, stream: &str) -> Result<StreamId> {
        let mut cmd = redis::cmd("XINFO");
        cmd.arg("STREAM").arg(stream);
        let value: Value = self
            .connection
            .query(&mut cmd)
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
        let mut cmd = redis::cmd("XRANGE");
        cmd.arg(stream).arg(start).arg("+").arg("COUNT").arg(count);
        let entries: Vec<(String, BTreeMap<String, Vec<u8>>)> = self
            .connection
            .query(&mut cmd)
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

    pub async fn xread_stream(
        &mut self,
        stream: &str,
        position: StreamId,
    ) -> Result<Vec<RawStreamMessage>> {
        let mut cmd = xread_stream_command(stream, position);
        let value: Value = self
            .connection
            .query(&mut cmd)
            .await
            .with_context(|| format!("failed to read XREAD for `{stream}`"))?;
        raw_messages_from_xread(value)
    }

    pub async fn xinfo_groups(&mut self, stream: &str) -> Result<Vec<GroupMetric>> {
        let mut cmd = redis::cmd("XINFO");
        cmd.arg("GROUPS").arg(stream);
        let value: Value = self
            .connection
            .query(&mut cmd)
            .await
            .with_context(|| format!("failed to read XINFO GROUPS for `{stream}`"))?;
        group_metrics_from_xinfo_groups(stream, value)
    }

    pub async fn xpending_summary(&mut self, stream: &str, group: &str) -> Result<PendingSummary> {
        let mut cmd = redis::cmd("XPENDING");
        cmd.arg(stream).arg(group);
        let value: Value = self
            .connection
            .query(&mut cmd)
            .await
            .with_context(|| format!("failed to read XPENDING for `{stream}` / `{group}`"))?;
        pending_summary_from_xpending(stream, group, value)
    }

    pub async fn xadd(&mut self, stream: &str, fields: &BTreeMap<String, Vec<u8>>) -> Result<()> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");
        for (key, value) in fields {
            cmd.arg(key).arg(value);
        }
        let _: String = self
            .connection
            .query(&mut cmd)
            .await
            .with_context(|| format!("failed to XADD to `{stream}`"))?;
        Ok(())
    }
}

pub fn xread_stream_command(stream: &str, position: StreamId) -> redis::Cmd {
    let mut cmd = redis::cmd("XREAD");
    cmd.arg("COUNT")
        .arg(128)
        .arg("STREAMS")
        .arg(stream)
        .arg(position.to_string());
    cmd
}

pub fn last_generated_id_from_xinfo(value: Value) -> Result<StreamId> {
    let items: Vec<Value> = FromRedisValue::from_redis_value(value)?;
    for pair in items.chunks(2) {
        if pair.len() != 2 {
            continue;
        }
        let key = redis_string(&pair[0])?;
        if key == "last-generated-id" {
            let id = redis_string(&pair[1])?;
            return Ok(id.parse()?);
        }
    }
    Ok(StreamId::ZERO)
}

pub fn raw_messages_from_xread(value: Value) -> Result<Vec<RawStreamMessage>> {
    let mut messages = Vec::new();
    let Value::Array(streams) = value else {
        return Ok(messages);
    };

    for stream_value in streams {
        let Value::Array(stream_tuple) = stream_value else {
            continue;
        };
        if stream_tuple.len() != 2 {
            continue;
        }
        let stream_name = redis_string(&stream_tuple[0])?;
        let Value::Array(entries) = &stream_tuple[1] else {
            continue;
        };

        for entry_value in entries {
            let Value::Array(entry_tuple) = entry_value else {
                continue;
            };
            if entry_tuple.len() != 2 {
                continue;
            }
            let id = redis_string(&entry_tuple[0])?.parse()?;
            let Value::Array(field_values) = &entry_tuple[1] else {
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
    let Value::Array(group_values) = value else {
        return Ok(groups);
    };

    for group_value in group_values {
        let Value::Array(fields) = group_value else {
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
    let Value::Array(values) = value else {
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
        Value::BulkString(bytes) => Ok(String::from_utf8(bytes.clone())?),
        Value::SimpleString(text) => Ok(text.clone()),
        Value::Okay => Ok("OK".to_string()),
        Value::Int(number) => Ok(number.to_string()),
        _ => anyhow::bail!("expected Redis string value"),
    }
}

fn redis_bytes(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::BulkString(bytes) => Ok(bytes.clone()),
        Value::SimpleString(text) => Ok(text.as_bytes().to_vec()),
        Value::Okay => Ok(b"OK".to_vec()),
        Value::Int(number) => Ok(number.to_string().into_bytes()),
        _ => anyhow::bail!("expected Redis bytes value"),
    }
}

fn redis_i64(value: &Value) -> Result<i64> {
    match value {
        Value::Int(number) => Ok(*number),
        Value::BulkString(bytes) => Ok(std::str::from_utf8(bytes)?.parse()?),
        Value::SimpleString(text) => Ok(text.parse()?),
        _ => anyhow::bail!("expected Redis integer value"),
    }
}

fn redis_u64(value: &Value) -> Result<u64> {
    let number = redis_i64(value)?;
    if number < 0 {
        anyhow::bail!("expected non-negative Redis integer value");
    }
    Ok(number as u64)
}
