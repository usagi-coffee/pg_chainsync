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
                .block_jobs()
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>();

            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                match job.connect_evm().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!("sync: blocks: {}: ws: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                match build_stream(&job).await {
                    Ok(stream) => {
                        log!("sync: blocks: {} started listening", job.id);
                        channel.send(Message::UpdateJob(
                            JobStatus::Running,
                            Arc::clone(job),
                        ));

                        map.insert(i, StreamNotifyClose::new(stream));
                    }
                    Err(err) => {
                        warning!("sync: blocks: {}: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            log!("sync: blocks: started listening");

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
                                "sync: blocks: stream {} has ended, restarting providers",
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
    log!("sync: blocks: {}: found {}", job.options.chain, number);

    channel.send(Message::Block(Block::EvmBlock(block), Arc::clone(job)));
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: Message) {
    let Message::Block(block, job) = message else {
        return;
    };

    match block {
        Block::EvmBlock(block) => handle_evm_message(block, job),
    }
}

pub fn handle_evm_message(block: alloy::rpc::types::Header, job: Arc<Job>) {
    let chain = job.options.chain;
    let number = block.number;
    log!("sync: blocks: {}: adding {}", chain, number);

    let handler = job
        .options
        .block_handler
        .as_ref()
        .expect("sync: blocks: missing handler");

    let id = job.id;

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            block
                .call_handler(&handler, id)
                .expect("sync: blocks: failed to call the handler {}")
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            warning!("sync: blocks: failed to call handler for {}", number);
        })
        .catch_others(|e| {
            log!("{:?}", e);
            warning!("sync: blocks: handler failed to put {}", number);
        })
        .execute();
    });
}

pub fn check_one(number: &u64, handler: &String, job: i64) -> bool {
    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            let found = Spi::get_one_with_args::<i64>(
                format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
                vec![
                    (
                        PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                        (*number as i64).into_datum(),
                    ),
                    (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
                ],
            );

            match found {
                Ok(value) => value.is_some(),
                Err(..) => false,
            }
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            false
        })
        .catch_others(|e| {
            log!("{:?}", e);
            false
        })
        .execute()
    })
}

pub async fn build_stream(
    job: &Job,
) -> anyhow::Result<SubscriptionStream<Header>> {
    let provider = job.connect_evm().await.context("Invalid provider")?;
    let sub = provider.subscribe_blocks().await?;
    Ok(sub.into_stream())
}
