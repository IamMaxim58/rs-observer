use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::catalog::{PhysicalStream, StreamCatalog};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub redis: RedisConfig,
    #[serde(default)]
    pub decoders: BTreeMap<String, DecoderConfig>,
    #[serde(default)]
    pub streams: Vec<SingleStreamConfig>,
    #[serde(default)]
    pub sharded_streams: Vec<ShardedStreamConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DecoderConfig {
    Raw,
    Prost(ProstDecoderConfig),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProstDecoderConfig {
    pub message: String,
    pub payload_field: String,
}

impl DecoderConfig {
    pub fn payload_field(&self) -> Option<&str> {
        match self {
            Self::Raw => None,
            Self::Prost(config) => Some(&config.payload_field),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SingleStreamConfig {
    pub name: String,
    #[serde(default = "default_decoder")]
    pub decoder: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardedStreamConfig {
    pub name: String,
    pub prefix: String,
    pub start: u16,
    pub end: u16,
    pub width: usize,
    #[serde(default = "default_decoder")]
    pub decoder: String,
}

fn default_decoder() -> String {
    "raw".to_string()
}

impl AppConfig {
    pub fn from_yaml(input: &str) -> Result<Self> {
        serde_yaml::from_str(input).context("failed to parse config")
    }

    pub fn catalog(&self) -> Result<StreamCatalog> {
        let mut physical = Vec::new();

        for stream in &self.streams {
            physical.push(PhysicalStream {
                name: stream.name.clone(),
                logical_name: stream.name.clone(),
                shard: None,
                decoder: stream.decoder.clone(),
            });
        }

        for stream in &self.sharded_streams {
            for shard in stream.start..=stream.end {
                physical.push(PhysicalStream {
                    name: format!("{}{:0width$}", stream.prefix, shard, width = stream.width),
                    logical_name: stream.name.clone(),
                    shard: Some(shard),
                    decoder: stream.decoder.clone(),
                });
            }
        }

        StreamCatalog::from_physical(physical)
    }
}
