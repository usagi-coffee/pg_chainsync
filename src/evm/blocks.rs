use pgrx::{log, warning};

use anyhow::Context;

use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{sleep_until, timeout, Duration, Instant};
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose};

use bus::BusReader;

use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::Header;

use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'sync: loop {
        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        if !channel.send(Message::Jobs(tx)) {
            break;
        }

        if let Ok(jobs) = rx.await {
            let mut jobs = jobs
                .evm_jobs()
                .block_jobs()
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>();

            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                match job.connect_evm().await {
                    Ok(_) => {}
                    Err(error) => {
                        warning!(
                            "sync: evm: blocks: {}: listener failed to connect with {}",
                            &job.name,
                            error
                        );
                        continue 'sync;
                    }
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                match build_stream(&job).await {
                    Ok(stream) => {
                        log!(
                            "sync: evm: blocks: {}: started listening",
                            &job.name
                        );

                        channel.send(Message::UpdateJob(
                            JobStatus::Running,
                            Arc::clone(job),
                        ));

                        map.insert(i, StreamNotifyClose::new(stream));
                    }
                    Err(error) => {
                        warning!("sync: evm: blocks: {}: failed to build stream with {}", &job.name, error);
                        continue 'sync;
                    }
                }
            }

            log!(
                "sync: evm: blocks: started listening to {} jobs",
                jobs.len()
            );

            let mut drain = false;
            loop {
                match signals.try_recv() {
                    Ok(signal) => {
                        if signal == Signal::RestartBlocks {
                            drain = true;
                        }
                    }
                    Err(..) => {}
                }

                if map.is_empty() {
                    sleep_until(Instant::now() + Duration::from_millis(100))
                        .await;

                    if drain {
                        continue 'sync;
                    }
                    continue;
                }

                match timeout(Duration::from_secs(1), map.next()).await {
                    Ok(Some(tick)) => {
                        let (i, block) = tick;
                        let job = &jobs[i];

                        if block.is_none() {
                            warning!(
                                "sync: evm: blocks: {}: stream has ended, restarting providers",
                                &job.name
                            );
                            continue 'sync;
                        }

                        // SAFETY: unwrap is safe because we checked for None
                        handle_block(job, block.unwrap(), &channel).await;
                    }
                    // If it took more than 1 second the stream is probably drained so we can "almost"
                    // safely restart the providers
                    _ => {
                        if drain {
                            continue 'sync;
                        }
                    }
                }
            }
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
