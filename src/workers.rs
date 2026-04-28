use std::collections::BTreeMap;

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use crate::app::WorkerEvent;
use crate::catalog::StreamCatalog;
use crate::config::RedisConfig;
use crate::redis_client::RedisObserverClient;
use crate::session::Session;
use crate::stream_id::StreamId;

pub fn zero_baseline_session(name: impl Into<String>, catalog: &StreamCatalog) -> Session {
    let baselines = catalog
        .physical_streams()
        .into_iter()
        .map(|stream| (stream.name.clone(), StreamId::ZERO))
        .collect::<BTreeMap<_, _>>();
    Session::new(name, baselines)
}

pub async fn bootstrap_baselines(
    config: &RedisConfig,
    catalog: &StreamCatalog,
) -> (BTreeMap<String, StreamId>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut baselines = BTreeMap::new();
    let mut client = match RedisObserverClient::connect(config).await {
        Ok(client) => Some(client),
        Err(error) => {
            warnings.push(format!("Redis unavailable: {error}"));
            None
        }
    };

    for stream in catalog.physical_streams() {
        let baseline = if let Some(client) = &mut client {
            match client.xinfo_stream_last_id(&stream.name).await {
                Ok(id) => id,
                Err(error) => {
                    warnings.push(format!(
                        "Using 0-0 baseline for `{}` after XINFO error: {error}",
                        stream.name
                    ));
                    StreamId::ZERO
                }
            }
        } else {
            StreamId::ZERO
        };
        baselines.insert(stream.name.clone(), baseline);
    }

    (baselines, warnings)
}

pub fn spawn_tail_worker(
    config: RedisConfig,
    catalog: StreamCatalog,
    initial_positions: BTreeMap<String, StreamId>,
) -> mpsc::UnboundedReceiver<WorkerEvent> {
    let (sender, receiver) = mpsc::unbounded_channel();
    let metrics_sender = sender.clone();
    let metrics_config = config.clone();
    tokio::spawn(async move {
        tail_loop(config, initial_positions, sender).await;
    });
    tokio::spawn(async move {
        metrics_loop(metrics_config, catalog, metrics_sender).await;
    });
    receiver
}

async fn tail_loop(
    config: RedisConfig,
    mut positions: BTreeMap<String, StreamId>,
    sender: mpsc::UnboundedSender<WorkerEvent>,
) {
    loop {
        let mut client = match RedisObserverClient::connect(&config).await {
            Ok(client) => client,
            Err(error) => {
                let _ = sender.send(WorkerEvent::WorkerError(format!(
                    "Redis tail connection failed: {error}"
                )));
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        loop {
            match client.xread_block(&positions, 1000).await {
                Ok(messages) => {
                    for message in messages {
                        positions.insert(message.stream.clone(), message.id);
                        if sender.send(WorkerEvent::MessageObserved(message)).is_err() {
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = sender.send(WorkerEvent::WorkerWarning(format!(
                        "Redis tail read failed: {error}"
                    )));
                    sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
        }
    }
}

async fn metrics_loop(
    config: RedisConfig,
    catalog: StreamCatalog,
    sender: mpsc::UnboundedSender<WorkerEvent>,
) {
    loop {
        let mut client = match RedisObserverClient::connect(&config).await {
            Ok(client) => client,
            Err(error) => {
                let _ = sender.send(WorkerEvent::WorkerWarning(format!(
                    "Redis metrics connection failed: {error}"
                )));
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        loop {
            for stream in catalog.physical_streams() {
                match client.xinfo_groups(&stream.name).await {
                    Ok(groups) => {
                        for group in groups {
                            let stream_name = group.stream.clone();
                            let group_name = group.group.clone();
                            let _ = sender.send(WorkerEvent::GroupMetricUpdated(group));
                            match client.xpending_summary(&stream_name, &group_name).await {
                                Ok(summary) => {
                                    let _ =
                                        sender.send(WorkerEvent::PendingSummaryUpdated(summary));
                                }
                                Err(error) => {
                                    let _ = sender.send(WorkerEvent::WorkerWarning(format!(
                                        "XPENDING failed for `{stream_name}` / `{group_name}`: {error}"
                                    )));
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(WorkerEvent::WorkerWarning(format!(
                            "XINFO GROUPS failed for `{}`: {error}",
                            stream.name
                        )));
                    }
                }
            }

            sleep(Duration::from_secs(2)).await;
        }
    }
}
