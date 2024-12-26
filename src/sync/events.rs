use pgrx::prelude::*;
use pgrx::{log, warning};

use anyhow::Context;

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

        if let Ok(mut jobs) = rx.await {
            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                if job.kind != JobKind::Events || job.oneshot {
                    continue;
                }

                if let Err(err) = job.connect().await {
                    warning!("sync: events: {}: ws: {}", job.id, err);
                    continue 'sync;
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                if job.kind != JobKind::Events || job.oneshot {
                    continue;
                }

                if job.ws.is_none() {
                    continue 'sync;
                }

                match build_stream(&job).await {
                    Ok(stream) => {
                        map.insert(i, StreamNotifyClose::new(stream));
                        channel.send(Message::UpdateJob(
                            job.id,
                            JobStatus::Running,
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
                        handle_log(job, log.unwrap(), &channel).await;
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

pub async fn handle_log(
    job: &Job,
    log: alloy::rpc::types::Log,
    channel: &Channel,
) {
    let chain = &job.chain;

    let block = log.block_number.unwrap();

    if log.log_index.is_none() {
        log!("sync: events: found pending {}", block);
        return;
    }

    let transaction = log.transaction_hash.unwrap();
    let index = log.log_index.unwrap();

    log!(
        "sync: events: {}: found {}<{}> at {}",
        chain,
        transaction,
        index,
        block
    );

    if let Some(options) = &job.options {
        // Await for block logic
        if let Some(options) = &options.await_block {
            let AwaitBlock {
                check_handler,
                block_handler,
            } = &options;

            let (tx, rx) = oneshot::channel::<bool>();

            if channel.send(Message::CheckBlock(
                *chain,
                block,
                check_handler.as_ref().unwrap().clone(),
                tx,
            )) {
                let found = rx.await;
                if found.is_ok() && found.unwrap() == false {
                    // Retry finding block until available
                    let mut retries = 0;
                    loop {
                        if retries > 20 {
                            panic!("sync: events: too many retries to get the block...");
                        }

                        if let Ok(Some(block)) = job
                            .ws
                            .as_ref()
                            .unwrap()
                            .get_block(
                                block.into(),
                                alloy::rpc::types::BlockTransactionsKind::Hashes
                            )
                            .await
                        {
                            channel.send(Message::Block(
                                *chain,
                                block.header,
                                block_handler.as_ref().unwrap().clone(),
                                Some(job.id),
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
    }

    if !channel.send(Message::Event(
        *chain,
        log,
        job.callback.clone(),
        Some(job.id),
    )) {
        warning!(
            "sync: events: {}: failed to send {}<{}>",
            job.chain,
            transaction,
            index
        )
    }
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: &Message) {
    let Message::Event(chain, log, callback, job) = message else {
        return;
    };

    let transaction = log.transaction_hash.unwrap();
    let index = log.log_index.unwrap();

    log!("sync: events: {}: adding {}<{}>", chain, transaction, index);

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            log.call_handler(&chain, &callback, &job.unwrap_or(-1))
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

pub fn build_filter(options: &JobOptions) -> Filter {
    let mut filter = Filter::new().from_block(BlockNumberOrTag::Latest);

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
        filter = filter.to_block::<u64>((*to_block as u64).into());
    }

    filter
}

pub async fn build_stream(
    job: &Job,
) -> anyhow::Result<SubscriptionStream<Log>> {
    let options = job.options.as_ref().context("Invalid options")?;
    let filter = build_filter(&options);

    let provider = job.ws.as_ref().context("Invalid provider")?;
    let sub = provider.subscribe_logs(&filter).await?;
    Ok(sub.into_stream())
}
