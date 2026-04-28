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
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub cluster_urls: Vec<String>,
}

impl RedisConfig {
    pub fn is_cluster(&self) -> bool {
        !self.cluster_urls.is_empty()
    }

    pub fn initial_urls(&self) -> Vec<&str> {
        if self.is_cluster() {
            self.cluster_urls.iter().map(String::as_str).collect()
        } else {
            self.url.iter().map(String::as_str).collect()
        }
    }

    pub fn single_url(&self) -> Result<&str> {
        self.url
            .as_deref()
            .context("redis.url is required when redis.cluster_urls is not configured")
    }
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
