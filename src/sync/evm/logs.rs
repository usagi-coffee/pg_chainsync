use pgrx::prelude::*;
use pgrx::{log, warning};

use std::sync::Arc;

use alloy::core::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::{BlockNumberOrTag, Filter};

use tokio::sync::oneshot;
use tokio::time::{sleep, sleep_until, timeout, Duration, Instant};
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose};

use bus::BusReader;

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
                .log_jobs()
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>();

            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                if let Err(err) = job.connect_evm().await {
                    warning!("sync: evm: logs: {}: ws: {}", job.id, err);
                    continue 'sync;
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                match build_stream(&job).await {
                    Ok(stream) => {
                        map.insert(i, StreamNotifyClose::new(stream));
                        channel.send(Message::UpdateJob(
                            JobStatus::Running,
                            Arc::clone(job),
                        ));

                        log!("sync: evm: log: {}: started listening", job.id);
                    }
                    Err(err) => {
                        warning!("sync: evm: log: {}: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            log!("sync: evm: log: started listening to {} jobs", jobs.len());

            let mut drain = false;
            loop {
                match signals.try_recv() {
                    Ok(signal) => {
                        if signal == Signal::RestartLogs {
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
                        let (i, log) = tick;
                        let job = &jobs[i];

                        if log.is_none() {
                            warning!("sync: evm: log: stream {} has ended, restarting providers", job.id);
                            continue 'sync;
                        }

                        // SAFETY: unwrap is safe because we checked for None
                        handle_evm_log(&job, log.unwrap(), &channel).await;
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

pub async fn handle_evm_log(
    job: &Arc<Job>,
    log: alloy::rpc::types::Log,
    channel: &Channel,
) {
    let block = log.block_number.unwrap();

    if log.log_index.is_none() {
        log!("sync: evm: logs: found pending {}", block);
        return;
    }

    let transaction = log.transaction_hash.unwrap();
    let index = log.log_index.unwrap();

    log!(
        "sync: evm: logs: {}: found {}<{}> at {}",
        &job.name,
        transaction,
        index,
        block
    );

    // Await for block logic
    if matches!(job.options.await_block, Some(true)) {
        let (tx, rx) = oneshot::channel::<bool>();

        if channel.send(Message::CheckBlock(block, tx, Arc::clone(job))) {
            let found = rx.await;

            if found.unwrap() == false {
                // Retry finding block until available
                let mut retries = 0;
                loop {
                    if retries > 20 {
                        error!("sync: evm: logs: {}: too many retries to get the block...", &job.name);
                    }

                    if let Ok(Some(block)) = job
                        .connect_evm()
                        .await
                        .unwrap()
                        .get_block(
                            block.into(),
                            alloy::rpc::types::BlockTransactionsKind::Hashes,
                        )
                        .await
                    {
                        channel.send(Message::EvmBlock(
                            block.header,
                            Arc::clone(job),
                        ));
                        break;
                    }

                    log!(
                        "sync: evm: logs: {}: could not find block {}, retrying",
                        &job.name,
                        block
                    );
                    sleep(Duration::from_millis(1000)).await;
                    retries = retries + 1;
                }
            }
        }
    }

    if !channel.send(Message::EvmLog(log, Arc::clone(job))) {
        warning!(
            "sync: evm: logs: {}: failed to send {}<{}>",
            &job.name,
            transaction,
            index
        )
    }
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: Message) {
    let Message::EvmLog(log, job) = message else {
        return;
    };

    handle_evm_message(log, job)
}

pub fn handle_evm_message(log: alloy::rpc::types::Log, job: Arc<Job>) {
    let transaction = log.transaction_hash.unwrap();
    let index = log.log_index.unwrap();

    log!(
        "sync: evm: logs: {}: adding {}<{}>",
        &job.name,
        transaction,
        index
    );

    let handler = job
        .options
        .log_handler
        .as_ref()
        .expect("sync: evm: logs: missing handler");
    let id = job.id;

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            log.call_handler(&handler, id)
                .expect("sync: evm: logs: failed to call the handler");
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            warning!(
                "sync: evm: logs: failed to call handler for {}<{}>",
                transaction,
                index
            );
        })
        .catch_others(|e| {
            log!("{:?}", e);
            warning!(
                "sync: evm: logs: handler failed to put {}<{}>",
                transaction,
                index
            );
        })
        .execute();
    });
}

pub fn build_filter(options: &JobOptions, block: u64) -> Filter {
    let mut filter = Filter::new();
    filter = filter.from_block(BlockNumberOrTag::Latest);

    if let Some(address) = &options.address {
        filter = filter.address(address.parse::<Address>().unwrap());
    }

    if let Some(event) = &options.event {
        filter = filter.event(event);
    }

    if let Some(topic0) = &options.topic0 {
        filter = filter.event_signature(topic0.parse::<B256>().unwrap());
    }

    if let Some(topic1) = &options.topic1 {
        filter = filter.topic1(topic1.parse::<B256>().unwrap());
    }

    if let Some(topic2) = &options.topic2 {
        filter = filter.topic2(topic2.parse::<B256>().unwrap());
    }

    if let Some(topic3) = &options.topic3 {
        filter = filter.topic3(topic3.parse::<B256>().unwrap());
    }

    if let Some(from_block) = &options.from_block {
        filter = filter.from_block::<u64>((*from_block as u64).into());
    }

    if let Some(to_block) = &options.to_block {
        if *to_block == 0 {
            filter = filter.to_block(BlockNumberOrTag::Safe);
        } else if *to_block < 0 {
            let target: i64 = block as i64 + *to_block;
            if target > 0 {
                filter = filter.to_block::<u64>((target as u64).into());
            }
        } else if *to_block > 0 {
            filter = filter.to_block::<u64>((*to_block as u64).into());
        }
    }

    filter
}

pub async fn build_stream(
    job: &Job,
) -> anyhow::Result<SubscriptionStream<alloy::rpc::types::Log>> {
    let ws = job.connect_evm().await.unwrap();
    let block = ws
        .get_block_number()
        .await
        .expect("failed to retrieve latest block") as u64;

    let filter = build_filter(&job.options, block);
    let sub = job
        .connect_evm()
        .await
        .unwrap()
        .subscribe_logs(&filter)
        .await?;
    Ok(sub.into_stream())
}
