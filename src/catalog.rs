use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamKind {
    Single,
    Sharded { shard_count: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalStream {
    pub name: String,
    pub logical_name: String,
    pub shard: Option<u16>,
    pub decoder: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalStream {
    pub name: String,
    pub kind: StreamKind,
    members: Vec<String>,
}

impl LogicalStream {
    pub fn physical_names(&self) -> Vec<String> {
        self.members.clone()
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }
}

#[derive(Debug, Clone)]
pub struct StreamCatalog {
    physical: BTreeMap<String, PhysicalStream>,
    logical: BTreeMap<String, LogicalStream>,
}

impl StreamCatalog {
    pub fn from_physical(streams: Vec<PhysicalStream>) -> Result<Self> {
        let mut seen = BTreeSet::new();
        let mut by_logical: BTreeMap<String, Vec<PhysicalStream>> = BTreeMap::new();
        let mut physical = BTreeMap::new();

        for stream in streams {
            if stream.name.trim().is_empty() || stream.logical_name.trim().is_empty() {
                bail!("empty stream name is not allowed");
            }
            if !seen.insert(stream.name.clone()) {
                bail!("duplicate physical stream `{}`", stream.name);
            }
            by_logical
                .entry(stream.logical_name.clone())
                .or_default()
                .push(stream.clone());
            physical.insert(stream.name.clone(), stream);
        }

        let logical = by_logical
            .into_iter()
            .map(|(name, mut members)| {
                members.sort_by(|left, right| left.name.cmp(&right.name));
                let kind = if members.len() == 1 && members[0].shard.is_none() {
                    StreamKind::Single
                } else {
                    StreamKind::Sharded {
                        shard_count: members.len(),
                    }
                };
                let member_names = members.into_iter().map(|stream| stream.name).collect();
                (
                    name.clone(),
                    LogicalStream {
                        name,
                        kind,
                        members: member_names,
                    },
                )
            })
            .collect();

        Ok(Self { physical, logical })
    }

    pub fn physical(&self, name: &str) -> Option<&PhysicalStream> {
        self.physical.get(name)
    }

    pub fn logical(&self, name: &str) -> Option<&LogicalStream> {
        self.logical.get(name)
    }

    pub fn logical_streams(&self) -> Vec<&LogicalStream> {
        self.logical.values().collect()
    }

    pub fn physical_streams(&self) -> Vec<&PhysicalStream> {
        self.physical.values().collect()
    }
}
