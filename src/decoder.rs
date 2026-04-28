use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use crate::config::{DecoderConfig, ProstDecoderConfig};
use crate::proto_registry;
use crate::stream_id::StreamId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawStreamMessage {
    pub stream: String,
    pub id: StreamId,
    pub fields: BTreeMap<String, Vec<u8>>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedMessage {
    pub stream: String,
    pub logical_stream: String,
    pub shard: Option<u16>,
    pub id: StreamId,
    pub message_type: String,
    pub decoded: Value,
    pub raw_fields: BTreeMap<String, Vec<u8>>,
    pub observed_at: DateTime<Utc>,
    pub decode_error: Option<String>,
}

pub trait Decoder {
    fn decode(&self, message: &RawStreamMessage) -> Result<(String, Value)>;
}

pub trait DecoderRegistry {
    fn decode(&self, decoder_id: &str, message: &RawStreamMessage) -> Result<(String, Value)>;
}

#[derive(Debug, Clone, Copy)]
pub struct RawJsonDecoder;

impl Decoder for RawJsonDecoder {
    fn decode(&self, message: &RawStreamMessage) -> Result<(String, Value)> {
        let mut fields = Map::new();
        for (key, value) in &message.fields {
            let rendered = match std::str::from_utf8(value) {
                Ok(text) => json!(text),
                Err(_) => json!({ "hex": hex::encode(value) }),
            };
            fields.insert(key.clone(), rendered);
        }

        Ok(("raw".to_string(), Value::Object(fields)))
    }
}

impl DecoderRegistry for RawJsonDecoder {
    fn decode(&self, _decoder_id: &str, message: &RawStreamMessage) -> Result<(String, Value)> {
        Decoder::decode(self, message)
    }
}

#[derive(Debug, Clone)]
pub struct StaticProstDecoderRegistry {
    configs: BTreeMap<String, DecoderConfig>,
    raw: RawJsonDecoder,
}

impl StaticProstDecoderRegistry {
    pub fn from_configs(configs: BTreeMap<String, DecoderConfig>) -> Self {
        Self {
            configs,
            raw: RawJsonDecoder,
        }
    }

    fn decode_prost(
        &self,
        config: &ProstDecoderConfig,
        message: &RawStreamMessage,
    ) -> Result<(String, Value)> {
        let payload = message.fields.get(&config.payload_field).ok_or_else(|| {
            anyhow::anyhow!(
                "payload field `{}` is missing for protobuf decoder `{}`",
                config.payload_field,
                config.message
            )
        })?;

        Ok((
            config.message.clone(),
            proto_registry::decode_message(&config.message, payload)?,
        ))
    }
}

impl DecoderRegistry for StaticProstDecoderRegistry {
    fn decode(&self, decoder_id: &str, message: &RawStreamMessage) -> Result<(String, Value)> {
        match self.configs.get(decoder_id) {
            Some(DecoderConfig::Raw) | None => Decoder::decode(&self.raw, message),
            Some(DecoderConfig::Prost(config)) => self.decode_prost(config, message),
        }
    }
}
