use std::collections::{BTreeMap, VecDeque};

use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};

use crate::catalog::StreamCatalog;
use crate::decoder::{DecodedMessage, Decoder, DecoderRegistry, RawStreamMessage};
use crate::session::Session;
use crate::stream_id::StreamId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStatus {
    Idle,
    Ok,
    HotShard,
    DecodeError,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalProjectionSummary {
    pub name: String,
    pub shard: Option<u16>,
    pub new_count: u64,
    pub rate_per_second: f64,
    pub last_message_at: Option<DateTime<Utc>>,
    pub decode_errors: u64,
    pub group_lag: Option<i64>,
    pub pending_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProjectionSummary {
    pub name: String,
    pub new_count: u64,
    pub rate_per_second: f64,
    pub last_message_at: Option<DateTime<Utc>>,
    pub decode_errors: u64,
    pub group_lag: Option<i64>,
    pub pending_count: u64,
    pub shards: Vec<PhysicalProjectionSummary>,
    pub status: StreamStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMetric {
    pub stream: String,
    pub group: String,
    pub consumer_count: u64,
    pub pending_count: u64,
    pub lag: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSummary {
    pub stream: String,
    pub group: String,
    pub pending_count: u64,
    pub oldest_id: Option<StreamId>,
}

#[derive(Debug, Clone)]
struct PhysicalProjection {
    new_count: u64,
    decode_errors: u64,
    last_seen_id: Option<StreamId>,
    last_message_at: Option<DateTime<Utc>>,
    recent_observed_at: VecDeque<DateTime<Utc>>,
    ring: VecDeque<DecodedMessage>,
    groups: BTreeMap<String, GroupMetric>,
    pending: BTreeMap<String, PendingSummary>,
}

impl PhysicalProjection {
    fn new() -> Self {
        Self {
            new_count: 0,
            decode_errors: 0,
            last_seen_id: None,
            last_message_at: None,
            recent_observed_at: VecDeque::new(),
            ring: VecDeque::new(),
            groups: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    fn rate_per_second(&self) -> f64 {
        self.recent_observed_at.len() as f64 / 10.0
    }
}

#[derive(Debug, Clone)]
pub struct ProjectionStore {
    catalog: StreamCatalog,
    session: Session,
    ring_capacity: usize,
    physical: BTreeMap<String, PhysicalProjection>,
}

impl ProjectionStore {
    pub fn new(catalog: StreamCatalog, session: Session, ring_capacity: usize) -> Self {
        let physical = catalog
            .physical_streams()
            .into_iter()
            .map(|stream| (stream.name.clone(), PhysicalProjection::new()))
            .collect();
        Self {
            catalog,
            session,
            ring_capacity,
            physical,
        }
    }

    pub fn observe(&mut self, raw: RawStreamMessage, decoder: &impl Decoder) -> Result<()> {
        self.observe_decoded(raw, |message| decoder.decode(message))
    }

    pub fn observe_with_registry(
        &mut self,
        raw: RawStreamMessage,
        registry: &impl DecoderRegistry,
    ) -> Result<()> {
        let decoder_id = self
            .catalog
            .physical(&raw.stream)
            .map(|stream| stream.decoder.clone())
            .unwrap_or_else(|| "raw".to_string());
        self.observe_decoded(raw, |message| registry.decode(&decoder_id, message))
    }

    fn observe_decoded(
        &mut self,
        raw: RawStreamMessage,
        decode: impl FnOnce(&RawStreamMessage) -> Result<(String, serde_json::Value)>,
    ) -> Result<()> {
        if !self.session.includes(&raw.stream, raw.id) {
            return Ok(());
        }

        let stream = self
            .catalog
            .physical(&raw.stream)
            .ok_or_else(|| anyhow::anyhow!("unknown stream `{}`", raw.stream))?;
        let (message_type, decoded, decode_error) = match decode(&raw) {
            Ok((message_type, decoded)) => (message_type, decoded, None),
            Err(error) => (
                "decode_error".to_string(),
                serde_json::Value::Null,
                Some(error.to_string()),
            ),
        };

        let message = DecodedMessage {
            stream: raw.stream.clone(),
            logical_stream: stream.logical_name.clone(),
            shard: stream.shard,
            id: raw.id,
            message_type,
            decoded,
            raw_fields: raw.fields,
            observed_at: raw.observed_at,
            decode_error,
        };

        let projection = self
            .physical
            .get_mut(&raw.stream)
            .ok_or_else(|| anyhow::anyhow!("missing projection for `{}`", raw.stream))?;
        projection.new_count += 1;
        projection.last_seen_id = Some(message.id);
        if message.decode_error.is_some() {
            projection.decode_errors += 1;
        }
        projection.last_message_at = Some(message.observed_at);
        projection.recent_observed_at.push_back(message.observed_at);
        let cutoff = message.observed_at - Duration::seconds(10);
        while projection
            .recent_observed_at
            .front()
            .is_some_and(|observed_at| *observed_at < cutoff)
        {
            projection.recent_observed_at.pop_front();
        }
        projection.ring.push_back(message);
        while projection.ring.len() > self.ring_capacity {
            projection.ring.pop_front();
        }

        Ok(())
    }

    pub fn logical_summary(&self, logical_name: &str) -> Result<LogicalProjectionSummary> {
        let logical = self
            .catalog
            .logical(logical_name)
            .ok_or_else(|| anyhow::anyhow!("unknown logical stream `{logical_name}`"))?;
        let mut shards = Vec::new();
        for physical_name in logical.members() {
            let stream = self
                .catalog
                .physical(physical_name)
                .ok_or_else(|| anyhow::anyhow!("unknown stream `{physical_name}`"))?;
            let projection = self
                .physical
                .get(physical_name)
                .ok_or_else(|| anyhow::anyhow!("missing projection for `{physical_name}`"))?;
            shards.push(PhysicalProjectionSummary {
                name: physical_name.clone(),
                shard: stream.shard,
                new_count: projection.new_count,
                rate_per_second: projection.rate_per_second(),
                last_message_at: projection.last_message_at,
                decode_errors: projection.decode_errors,
                group_lag: optional_sum(projection.groups.values().filter_map(|group| group.lag)),
                pending_count: projection
                    .groups
                    .values()
                    .map(|group| group.pending_count)
                    .sum::<u64>()
                    + projection
                        .pending
                        .values()
                        .map(|pending| pending.pending_count)
                        .sum::<u64>(),
            });
        }

        if shards.is_empty() {
            bail!("logical stream `{logical_name}` has no physical members");
        }

        let new_count = shards.iter().map(|shard| shard.new_count).sum();
        let rate_per_second = shards.iter().map(|shard| shard.rate_per_second).sum();
        let decode_errors = shards.iter().map(|shard| shard.decode_errors).sum();
        let pending_count = shards.iter().map(|shard| shard.pending_count).sum();
        let group_lag = optional_sum(shards.iter().filter_map(|shard| shard.group_lag));
        let last_message_at = shards
            .iter()
            .filter_map(|shard| shard.last_message_at)
            .max();
        let status = status_for(&shards);

        Ok(LogicalProjectionSummary {
            name: logical_name.to_string(),
            new_count,
            rate_per_second,
            last_message_at,
            decode_errors,
            group_lag,
            pending_count,
            shards,
            status,
        })
    }

    pub fn logical_summaries(&self) -> Vec<LogicalProjectionSummary> {
        self.catalog
            .logical_streams()
            .into_iter()
            .filter_map(|logical| self.logical_summary(&logical.name).ok())
            .collect()
    }

    pub fn timeline(&self, logical_name: &str) -> Result<Vec<DecodedMessage>> {
        let logical = self
            .catalog
            .logical(logical_name)
            .ok_or_else(|| anyhow::anyhow!("unknown logical stream `{logical_name}`"))?;
        let mut messages = Vec::new();
        for physical_name in logical.members() {
            if let Some(projection) = self.physical.get(physical_name) {
                messages.extend(projection.ring.iter().cloned());
            }
        }
        messages.sort_by_key(|message| message.id);
        Ok(messages)
    }

    pub fn all_messages(&self) -> Vec<DecodedMessage> {
        let mut messages = self
            .physical
            .values()
            .flat_map(|projection| projection.ring.iter().cloned())
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.id);
        messages
    }

    pub fn last_seen_baselines(&self) -> BTreeMap<String, StreamId> {
        self.catalog
            .physical_streams()
            .into_iter()
            .map(|stream| {
                let id = self
                    .physical
                    .get(&stream.name)
                    .and_then(|projection| projection.last_seen_id)
                    .unwrap_or_else(|| self.session.baseline(&stream.name));
                (stream.name.clone(), id)
            })
            .collect()
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn update_group_metric(&mut self, metric: GroupMetric) {
        if let Some(projection) = self.physical.get_mut(&metric.stream) {
            projection.groups.insert(metric.group.clone(), metric);
        }
    }

    pub fn update_pending_summary(&mut self, summary: PendingSummary) {
        if let Some(projection) = self.physical.get_mut(&summary.stream) {
            projection.pending.insert(summary.group.clone(), summary);
        }
    }
}

fn optional_sum(values: impl Iterator<Item = i64>) -> Option<i64> {
    let mut saw_value = false;
    let mut total = 0;
    for value in values {
        saw_value = true;
        total += value;
    }
    saw_value.then_some(total)
}

fn status_for(shards: &[PhysicalProjectionSummary]) -> StreamStatus {
    if shards.iter().any(|shard| shard.decode_errors > 0) {
        return StreamStatus::DecodeError;
    }

    let total: u64 = shards.iter().map(|shard| shard.new_count).sum();
    if total == 0 {
        return StreamStatus::Idle;
    }

    if shards.len() > 1 {
        let max = shards
            .iter()
            .map(|shard| shard.new_count)
            .max()
            .unwrap_or(0);
        let average = total as f64 / shards.len() as f64;
        if max >= 10 && average > 0.0 && max as f64 >= average * 3.0 {
            return StreamStatus::HotShard;
        }
    }

    StreamStatus::Ok
}
