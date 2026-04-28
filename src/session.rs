use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::stream_id::StreamId;

#[derive(Debug, Clone)]
pub struct Session {
    pub id: Uuid,
    pub name: String,
    pub number: u64,
    pub started_at: DateTime<Utc>,
    baselines: BTreeMap<String, StreamId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionArchiveSummary {
    pub id: Uuid,
    pub name: String,
    pub number: u64,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
}

impl Session {
    pub fn new(name: impl Into<String>, baselines: BTreeMap<String, StreamId>) -> Self {
        Self::new_numbered(name, 1, baselines)
    }

    pub fn new_numbered(
        name: impl Into<String>,
        number: u64,
        baselines: BTreeMap<String, StreamId>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            number,
            started_at: Utc::now(),
            baselines,
        }
    }

    pub fn includes(&self, stream_name: &str, id: StreamId) -> bool {
        let baseline = self
            .baselines
            .get(stream_name)
            .copied()
            .unwrap_or(StreamId::ZERO);
        id.is_after(baseline)
    }

    pub fn baseline(&self, stream_name: &str) -> StreamId {
        self.baselines
            .get(stream_name)
            .copied()
            .unwrap_or(StreamId::ZERO)
    }

    pub fn baselines(&self) -> &BTreeMap<String, StreamId> {
        &self.baselines
    }

    pub fn archive_summary(&self, ended_at: DateTime<Utc>) -> SessionArchiveSummary {
        SessionArchiveSummary {
            id: self.id,
            name: self.name.clone(),
            number: self.number,
            started_at: self.started_at,
            ended_at,
        }
    }
}
