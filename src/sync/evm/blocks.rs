use pgrx::prelude::*;
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
                    Err(err) => {
                        warning!("sync: evm: blocks: {}: ws: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                match build_stream(&job).await {
                    Ok(stream) => {
                        log!("sync: evm: blocks: {} started listening", job.id);
                        channel.send(Message::UpdateJob(
                            JobStatus::Running,
                            Arc::clone(job),
                        ));

                        map.insert(i, StreamNotifyClose::new(stream));
                    }
                    Err(err) => {
                        warning!("sync: evm: blocks: {}: {}", job.id, err);
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
                                "sync: evm: blocks: stream {} has ended, restarting providers",
                                job.id
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

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: Message) {
    let Message::EvmBlock(block, job) = message else {
        return;
    };

    handle_evm_message(block, job)
}

pub fn handle_evm_message(block: alloy::rpc::types::Header, job: Arc<Job>) {
    let number = block.number;
    log!("sync: evm: blocks: {}: adding {}", &job.name, number);

    let handler = job
        .options
        .block_handler
        .as_ref()
        .expect("sync: evm: blocks: missing handler");

    let id = job.id;

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            block
                .call_handler(&handler, id)
                .expect("sync: evm: blocks: failed to call the handler {}");
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            warning!(
                "sync: evm: blocks: failed to call handler for {}",
                number
            );
        })
        .catch_others(|e| {
            log!("{:?}", e);
            warning!("sync: evm: blocks: handler failed to put {}", number);
        })
        .execute();
    });
}

pub async fn build_stream(
    job: &Job,
) -> anyhow::Result<SubscriptionStream<Header>> {
    let provider = job.connect_evm().await.context("Invalid provider")?;
    let sub = provider.subscribe_blocks().await?;
    Ok(sub.into_stream())
}
