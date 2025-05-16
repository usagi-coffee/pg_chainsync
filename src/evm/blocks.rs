use pgrx::{log, warning};

use anyhow::Context;

use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{sleep_until, Duration, Instant};
use tokio_stream::{StreamExt, StreamNotifyClose};

use bus::BusReader;

use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::Header;

use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

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

        for job in jobs {
            let channel = channel.clone();
            let handle = tokio::spawn(async move {
                let mut retries = 0;
                'job: loop {
                    if retries >= 10 {
                        warning!(
                        "sync: evm: blocks: {}: too many retries, stopping job",
                        &job.name
                    );
                        return;
                    }

                    if let Err(error) = job.connect_evm().await {
                        warning!(
                            "sync: evm: blocks: {}: failed to connect with provider with {}",
                            &job.name,
                            error
                        );

                        retries += 1;
                        sleep_until(
                            Instant::now() + Duration::from_millis(250),
                        )
                        .await;
                        continue;
                    };

                    let mut stream = match build_stream(&job).await {
                        Ok(stream) => StreamNotifyClose::new(stream),
                        Err(error) => {
                            warning!(
                            "sync: evm: blocks: {}: failed to build stream with {}",
                            &job.name,
                            error
                        );

                            retries += 1;
                            sleep_until(
                                Instant::now() + Duration::from_millis(250),
                            )
                            .await;
                            continue;
                        }
                    };

                    channel
                        .send(Message::UpdateJob(job.id, JobStatus::Running));

                    log!("sync: evm: blocks: {}: started listening", &job.name);
                    loop {
                        match stream.next().await {
                            Some(Some(block)) => {
                                handle_block(&job, block, &channel).await
                            }
                            _ => {
                                warning!(
                                    "sync: evm: blocks: {}: stream has ended, restarting provider",
                                    &job.name
                                );

                                continue 'job;
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
    block: alloy::rpc::types::Header,
    channel: &Channel,
) {
    let number = block.number;
    log!("sync: evm: blocks: {}: found {}", &job.name, number);

    channel.send(Message::EvmBlock(block, Arc::clone(job)));
}

pub async fn build_stream(
    job: &Job,
) -> anyhow::Result<SubscriptionStream<Header>> {
    let provider = job.connect_evm().await.context("Invalid provider")?;
    let sub = provider.subscribe_blocks().await?;
    Ok(sub.into_stream())
}
