use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use prost::Message;
use tokio::time::sleep;

use crate::catalog::{PhysicalStream, StreamCatalog};
use crate::config::{AppConfig, DecoderConfig};
use crate::proto_registry::observer::example::{ExampleEvent, ExampleEvent2};
use crate::redis_client::RedisObserverClient;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerMessage {
    pub stream: String,
    pub fields: BTreeMap<String, Vec<u8>>,
}

pub struct ProducerMessageFactory {
    streams: Vec<ProducerStream>,
    sequence: u64,
}

#[derive(Debug, Clone)]
struct ProducerStream {
    physical: PhysicalStream,
    message_name: String,
}

impl ProducerMessageFactory {
    pub fn new(config: &AppConfig, catalog: &StreamCatalog) -> Self {
        let streams = catalog
            .physical_streams()
            .into_iter()
            .filter_map(|physical| {
                let message_name = match config.decoders.get(&physical.decoder) {
                    Some(DecoderConfig::Prost(prost)) => prost.message.clone(),
                    Some(DecoderConfig::Raw) | None => return None,
                };
                Some(ProducerStream {
                    physical: physical.clone(),
                    message_name,
                })
            })
            .collect();

        Self {
            streams,
            sequence: 0,
        }
    }

    pub fn next_message(&mut self) -> Result<ProducerMessage> {
        if self.streams.is_empty() {
            anyhow::bail!("producer has no protobuf-configured streams");
        }

        let index = (self.sequence as usize) % self.streams.len();
        let stream = &self.streams[index];
        let payload = encode_example_payload(&stream.message_name, self.sequence)?;
        self.sequence += 1;

        Ok(ProducerMessage {
            stream: stream.physical.name.clone(),
            fields: BTreeMap::from([
                ("type".to_string(), stream.message_name.as_bytes().to_vec()),
                ("payload".to_string(), payload),
            ]),
        })
    }
}

pub async fn run_loop(config: AppConfig, interval: Duration) -> Result<()> {
    let catalog = config.catalog()?;
    let mut factory = ProducerMessageFactory::new(&config, &catalog);
    let mut client = RedisObserverClient::connect(&config.redis).await?;

    loop {
        let message = factory.next_message()?;
        client.xadd(&message.stream, &message.fields).await?;
        sleep(interval).await;
    }
}

fn encode_example_payload(message_name: &str, sequence: u64) -> Result<Vec<u8>> {
    match message_name {
        "observer.example.ExampleEvent" => Ok(ExampleEvent {
            event_type: if sequence.is_multiple_of(2) {
                "ExampleStarted".to_string()
            } else {
                "ExampleRetried".to_string()
            },
            event_id: format!("evt_{sequence:06}"),
            attempt: (sequence % 3 + 1) as u32,
        }
        .encode_to_vec()),
        "observer.example.ExampleEvent2" => Ok(ExampleEvent2 {
            event_type: if sequence.is_multiple_of(2) {
                "ExampleCompleted".to_string()
            } else {
                "ExampleRejected".to_string()
            },
            event_id: format!("evt_{sequence:06}"),
            success: !sequence.is_multiple_of(5),
        }
        .encode_to_vec()),
        other => anyhow::bail!("producer does not know how to build `{other}`"),
    }
}
