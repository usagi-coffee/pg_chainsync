use alloy::network::{AnyHeader, AnyRpcBlock};
use pgrx::{log, warning};

use anyhow::{Context, bail, ensure};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::{StreamExt, StreamNotifyClose};

use bus::BusReader;

use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;

use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

fn ingress_key(job: &Job) -> String {
    let ws = job.options.ws.as_deref().unwrap_or("<missing-ws>");
    format!("ws={}", ws)
}

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'blocks: loop {
        let mut handles = vec![];

        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        channel.send(Message::Jobs(tx));

        let Ok(jobs) = rx.await else {
            warning!("sync: evm: blocks: failed to get jobs");
            return;
        };

        let jobs = jobs
            .evm_jobs()
            .block_jobs()
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        log!("sync: evm: blocks: found {} jobs", jobs.len());

        let mut groups: HashMap<String, Vec<Arc<Job>>> = HashMap::new();
        for job in jobs {
            groups.entry(ingress_key(&job)).or_default().push(job);
        }

        log!("sync: evm: blocks: shared ingress groups: {}", groups.len());

        for (key, group_jobs) in groups {
            let Some(primary) = group_jobs.first().cloned() else {
                continue;
            };
            let channel = channel.clone();
            let handle = tokio::spawn(async move {
                let mut retries = 0;
                'group: loop {
                    if retries >= 10 {
                        warning!(
                            "sync: evm: blocks: {}: too many retries, stopping shared group",
                            key
                        );
                        return;
                    }

                    if let Err(error) = primary.connect_evm().await {
                        warning!(
                            "sync: evm: blocks: {}: failed to connect provider with {}",
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
                                "sync: evm: blocks: {}: failed to build shared stream with {}",
                                key,
                                error
                            );

                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(200))
                                .await;
                            continue;
                        }
                    };

                    for job in &group_jobs {
                        channel.send(Message::UpdateJob(
                            job.id,
                            JobStatus::Running,
                        ));
                    }

                    log!("sync: evm: blocks: {}: started shared listener", key);
                    loop {
                        match stream.next().await {
                            Some(Some(block)) => {
                                for job in &group_jobs {
                                    if let Err(error) = handle_block(
                                        job,
                                        block.clone(),
                                        &channel,
                                    )
                                    .await
                                    {
                                        warning!(
                                            "sync: evm: blocks: {}: {}: failed to handle shared block with {}",
                                            key,
                                            &job.name,
                                            error
                                        );
                                    }
                                }

                                retries = 0;
                            }
                            _ => {
                                warning!(
                                    "sync: evm: blocks: {}: shared stream ended, restarting",
                                    key
                                );

                                continue 'group;
                            }
                        }
                    }
                }
            });

            handles.push(handle);
        }

        loop {
            match signals.try_recv() {
                Ok(signal) => match signal {
                    Signal::RestartBlocks => {
                        log!("sync: evm: blocks: restarting jobs");
                        for handle in handles {
                            handle.abort();
                        }

                        continue 'blocks;
                    }
                    _ => {}
                },
                Err(_) => {}
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub async fn handle_block(
    job: &Arc<Job>,
    block: alloy::rpc::types::Header<AnyHeader>,
    channel: &Channel,
) -> Result<(), anyhow::Error> {
    let number = block.number;
    log!("sync: evm: blocks: {}: found {}", &job.name, number);

    ensure!(
        channel.send(Message::EvmBlock(block, job.clone())),
        "sync: evm: blocks: {}: failed to send block to channel",
        &job.name
    );

    Ok(())
}

pub async fn build_stream(
    job: &Job,
) -> anyhow::Result<SubscriptionStream<alloy::rpc::types::Header<AnyHeader>>> {
    let provider = job.connect_evm().await.context("Invalid provider")?;
    let sub = provider.subscribe_blocks().await?;
    Ok(sub.into_stream())
}

// Attempts to fetch a block by its number, retrying if necessary.
pub async fn try_block(
    block: u64,
    job: &Arc<Job>,
) -> Result<AnyRpcBlock, anyhow::Error> {
    let mut retries = 0;
    loop {
        if retries > 20 {
            bail!(
                "sync: evm: {}: too many retries to get the block...",
                &job.name
            );
        }

        // Reconnect ws on every block retry
        let Ok(client) = job.reconnect_evm().await else {
            warning!(
                "sync: evm: {}: failed to connect to evm at await block handler",
                &job.name
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
            retries = retries + 1;
            continue;
        };

        if let Ok(Some(block)) = client.get_block(block.into()).await {
            return Ok(block);
        }

        log!(
            "sync: evm: {}: could not find block {}, retrying",
            &job.name,
            block
        );

        tokio::time::sleep(Duration::from_millis(1000)).await;
        retries = retries + 1;
    }
}
