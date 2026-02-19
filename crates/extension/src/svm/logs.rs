use pgrx::warning;

use anyhow::{Context, bail, ensure};
use solana_client::rpc_config::{
    RpcTransactionLogsConfig, RpcTransactionLogsFilter,
};

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinHandle;

use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt, StreamNotifyClose};

use solana_client::rpc_response::{Response, RpcLogsResponse};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use bus::BusReader;

use crate::types::HandlerRuntime;

use crate::channel::Channel;
use crate::types::*;

fn ingress_key(handler: &HandlerRuntime) -> String {
    let rpc = handler.options.rpc.as_deref().unwrap_or("<missing-rpc>");
    let Some(options) = &handler.options.svm else {
        return format!("rpc={}:svm=none", rpc);
    };

    format!("rpc={}|mentions={:?}", rpc, options.mentions)
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
                    "sync: ingress: svm:logs: {}: too many retries, stopping lane",
                    key
                );

                return;
            }

            if let Err(error) = primary.reconnect_svm_ws().await {
                warning!(
                    "sync: ingress: svm:logs: {}: ws connect failed: {}",
                    key,
                    error
                );

                retries += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            };

            if let Err(error) = primary.connect_svm_rpc().await {
                warning!(
                    "sync: ingress: svm:logs: {}: rpc connect failed: {}",
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
                        "sync: ingress: svm:logs: {}: stream build failed: {}",
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
                    Some(Some(svm_log)) => {
                        for handler in &group_handlers {
                            if let Err(error) = handle_svm_log(
                                handler,
                                svm_log.clone(),
                                &channel,
                            )
                            .await
                            {
                                warning!(
                                    "sync: ingress: svm:logs: {}: route={} dispatch failed: {}",
                                    key,
                                    &handler.name,
                                    error
                                );
                            }
                        }
                    }
                    _ => {
                        warning!(
                            "sync: ingress: svm:logs: {}: stream ended, reconnecting",
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
        warning!("sync: ingress: svm:logs: failed to load route table");
        return None;
    };

    let handlers = handlers
        .svm_handlers()
        .log_handlers()
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

    let mut groups: HashMap<String, Vec<Arc<HandlerRuntime>>> = HashMap::new();
    for handler in handlers {
        groups
            .entry(ingress_key(&handler))
            .or_default()
            .push(handler);
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
            Ok(Signal::RestartLogs) => {
                let Some(groups) = desired_groups(&channel).await else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };

                let mut keep = HashMap::new();
                for (key, group_handlers) in groups {
                    let fingerprint = group_fingerprint(&group_handlers);
                    match running.remove(&key) {
                        Some((old_fingerprint, handle))
                            if old_fingerprint == fingerprint =>
                        {
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

pub async fn handle_svm_log(
    handler: &Arc<HandlerRuntime>,
    log: Response<RpcLogsResponse>,
    channel: &Channel,
) -> Result<(), anyhow::Error> {
    let Some(_) = &handler.options.svm else {
        bail!(
            "sync: ingress: svm:logs: {}: missing svm options",
            &handler.name
        );
    };

    ensure!(
        channel.send(Message::SvmLog(log, handler.clone())),
        "sync: ingress: svm:logs: {}: enqueue log failed",
        &handler.name
    );

    Ok(())
}

pub fn build_config(_: &SvmOptions) -> RpcTransactionLogsConfig {
    RpcTransactionLogsConfig {
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        }),
    }
}

pub fn build_filter(options: &SvmOptions) -> RpcTransactionLogsFilter {
    if let Some(mentions) = &options.mentions {
        return RpcTransactionLogsFilter::Mentions(
            mentions.iter().map(|m| m.to_string()).collect(),
        );
    }

    RpcTransactionLogsFilter::All
}

pub async fn build_stream<'a>(
    handler: &'a HandlerRuntime,
) -> anyhow::Result<
    Pin<Box<dyn Stream<Item = Response<RpcLogsResponse>> + 'a + Send>>,
> {
    let options = handler
        .options
        .svm
        .as_ref()
        .expect("SVM options are not set");
    let filter = build_filter(options);
    let provider =
        handler.connect_svm_ws().await.context("Invalid provider")?;
    let sub = provider
        .logs_subscribe(filter, build_config(options))
        .await?;
    Ok(sub.0)
}
