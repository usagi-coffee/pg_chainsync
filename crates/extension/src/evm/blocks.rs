use alloy::network::{AnyHeader, AnyRpcBlock};
use pgrx::warning;

use anyhow::{Context, bail, ensure};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::{StreamExt, StreamNotifyClose};

use bus::BusReader;

use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;

use crate::types::HandlerRuntime;

use crate::channel::Channel;
use crate::types::*;

fn ingress_key(handler: &HandlerRuntime) -> String {
    let ws = handler.options.ws.as_deref().unwrap_or("<missing-ws>");
    format!("ws={}", ws)
}

fn group_fingerprint(group_handlers: &[Arc<HandlerRuntime>]) -> String {
    let mut ids = group_handlers
        .iter()
        .map(|h| h.id.to_string())
        .collect::<Vec<_>>();
    ids.sort();
    ids.join(",")
}

fn spawn_group(
    key: String,
    group_handlers: Vec<Arc<HandlerRuntime>>,
    channel: Arc<Channel>,
) -> JoinHandle<()> {
    let Some(primary) = group_handlers.first().cloned() else {
        return tokio::spawn(async {});
    };

    tokio::spawn(async move {
        let mut retries = 0;
        'group: loop {
            if retries >= 10 {
                warning!(
                    "sync: ingress: evm:blocks: {}: too many retries, stopping lane",
                    key
                );
                return;
            }

            if let Err(error) = primary.connect_evm().await {
                warning!(
                    "sync: ingress: evm:blocks: {}: provider connect failed: {}",
                    key,
                    error
                );

                retries += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            };

            let mut stream = match build_stream(&primary).await {
                Ok(stream) => StreamNotifyClose::new(stream),
                Err(error) => {
                    warning!(
                        "sync: ingress: evm:blocks: {}: stream build failed: {}",
                        key,
                        error
                    );

                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };

            loop {
                match stream.next().await {
                    Some(Some(block)) => {
                        for handler in &group_handlers {
                            if let Err(error) =
                                handle_block(handler, block.clone(), &channel).await
                            {
                                warning!(
                                    "sync: ingress: evm:blocks: {}: route={} dispatch failed: {}",
                                    key,
                                    &handler.name,
                                    error
                                );
                            }
                        }

                        retries = 0;
                    }
                    _ => {
                        warning!(
                            "sync: ingress: evm:blocks: {}: stream ended, reconnecting",
                            key
                        );

                        continue 'group;
                    }
                }
            }
        }
    })
}

async fn desired_groups(
    channel: &Channel,
) -> Option<HashMap<String, Vec<Arc<HandlerRuntime>>>> {
    let (tx, rx) = oneshot::channel::<Vec<HandlerRuntime>>();
    channel.send(Message::Handlers(tx));

    let Ok(handlers) = rx.await else {
        warning!("sync: ingress: evm:blocks: failed to load route table");
        return None;
    };

    let handlers = handlers
        .evm_handlers()
        .block_handlers()
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

    let mut groups: HashMap<String, Vec<Arc<HandlerRuntime>>> = HashMap::new();
    for handler in handlers {
        groups.entry(ingress_key(&handler)).or_default().push(handler);
    }

    Some(groups)
}

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    let mut running: HashMap<String, (String, JoinHandle<()>)> = HashMap::new();

    let Some(initial_groups) = desired_groups(&channel).await else {
        return;
    };
    for (key, group_handlers) in initial_groups {
        let fingerprint = group_fingerprint(&group_handlers);
        let handle = spawn_group(key.clone(), group_handlers, channel.clone());
        running.insert(key, (fingerprint, handle));
    }

    loop {
        match signals.try_recv() {
            Ok(Signal::RestartBlocks) => {
                let Some(groups) = desired_groups(&channel).await else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };

                let mut keep = HashMap::new();
                for (key, group_handlers) in groups {
                    let fingerprint = group_fingerprint(&group_handlers);
                    match running.remove(&key) {
                        Some((old_fingerprint, handle)) if old_fingerprint == fingerprint => {
                            keep.insert(key, (old_fingerprint, handle));
                        }
                        Some((_, handle)) => {
                            handle.abort();
                            let new_handle = spawn_group(
                                key.clone(),
                                group_handlers,
                                channel.clone(),
                            );
                            keep.insert(key, (fingerprint, new_handle));
                        }
                        None => {
                            let new_handle = spawn_group(
                                key.clone(),
                                group_handlers,
                                channel.clone(),
                            );
                            keep.insert(key, (fingerprint, new_handle));
                        }
                    }
                }

                for (_, (_, handle)) in running {
                    handle.abort();
                }
                running = keep;
            }
            Ok(_) => {}
            Err(_) => {}
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn handle_block(
    handler: &Arc<HandlerRuntime>,
    block: alloy::rpc::types::Header<AnyHeader>,
    channel: &Channel,
) -> Result<(), anyhow::Error> {
    ensure!(
        channel.send(Message::EvmBlock(block, handler.clone())),
        "sync: ingress: evm:blocks: {}: enqueue failed",
        &handler.name
    );

    Ok(())
}

pub async fn build_stream(
    handler: &HandlerRuntime,
) -> anyhow::Result<SubscriptionStream<alloy::rpc::types::Header<AnyHeader>>> {
    let provider = handler.connect_evm().await.context("Invalid provider")?;
    let sub = provider.subscribe_blocks().await?;
    Ok(sub.into_stream())
}

// Attempts to fetch a block by its number, retrying if necessary.
pub async fn try_block(
    block: u64,
    handler: &Arc<HandlerRuntime>,
) -> Result<AnyRpcBlock, anyhow::Error> {
    let mut retries = 0;
    loop {
        if retries > 20 {
            bail!(
                "sync: ingress: evm:blocks: {}: too many retries fetching block",
                &handler.name
            );
        }

        // Reconnect ws on every block retry
        let Ok(client) = handler.reconnect_evm().await else {
            warning!(
                "sync: ingress: evm:blocks: {}: reconnect failed during await_block",
                &handler.name
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
            retries = retries + 1;
            continue;
        };

        if let Ok(Some(block)) = client.get_block(block.into()).await {
            return Ok(block);
        }

        tokio::time::sleep(Duration::from_millis(1000)).await;
        retries = retries + 1;
    }
}
