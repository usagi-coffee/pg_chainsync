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
                .event_jobs()
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>();

            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                if let Err(err) = job.connect_evm().await {
                    warning!("sync: events: {}: ws: {}", job.id, err);
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

                        log!("sync: events: {}: started listening", job.id);
                    }
                    Err(err) => {
                        warning!("sync: events: {}: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            log!("sync: events: started listening");

            let mut drain = false;
            loop {
                match signals.try_recv() {
                    Ok(signal) => {
                        if signal == Signal::RestartEvents {
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
                            warning!("sync: events: stream {} has ended, restarting providers", job.id);
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
        log!("sync: events: found pending {}", block);
        return;
    }

    let transaction = log.transaction_hash.unwrap();
    let index = log.log_index.unwrap();

    log!(
        "sync: events: {}: found {}<{}> at {}",
        job.options.chain,
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
                        panic!("sync: events: too many retries to get the block...");
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
                        channel.send(Message::Block(
                            Block::EvmBlock(block.header),
                            Arc::clone(job),
                        ));
                        break;
                    }

                    log!(
                        "sync: events: could not find block {}, retrying",
                        block
                    );
                    sleep(Duration::from_millis(1000)).await;
                    retries = retries + 1;
                }
            }
        }
    }

    if !channel.send(Message::Event(Log::EvmLog(log), Arc::clone(job))) {
        warning!(
            "sync: events: {}: failed to send {}<{}>",
            job.options.chain,
            transaction,
            index
        )
    }
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: Message) {
    let Message::Event(log, job) = message else {
        return;
    };

    match log {
        Log::EvmLog(log) => handle_evm_message(log, job),
    }
}

pub fn handle_evm_message(log: alloy::rpc::types::Log, job: Arc<Job>) {
    let chain = job.options.chain;
    let transaction = log.transaction_hash.unwrap();
    let index = log.log_index.unwrap();

    log!("sync: events: {}: adding {}<{}>", chain, transaction, index);

    let handler = job
        .options
        .event_handler
        .as_ref()
        .expect("sync: events: missing handler");
    let id = job.id;

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            log.call_handler(&handler, id)
                .expect("sync: events: failed to call the handler")
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            warning!(
                "sync: events: failed to call handler for {}<{}>",
                transaction,
                index
            );
        })
        .catch_others(|e| {
            log!("{:?}", e);
            warning!(
                "sync: events: handler failed to put {}<{}>",
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
                println!("using safe block {} from {}", target, block);
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
