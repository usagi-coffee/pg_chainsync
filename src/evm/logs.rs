use pgrx::prelude::*;
use pgrx::{log, warning};

use std::sync::Arc;

use alloy::core::primitives::{Address, B256};
use alloy::primitives::keccak256;
use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::{BlockNumberOrTag, Filter};

use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tokio_stream::{StreamExt, StreamNotifyClose};

use bus::BusReader;

use crate::channel::Channel;
use crate::types::*;

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'logs: loop {
        let mut handles = vec![];

        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        channel.send(Message::Jobs(tx));

        let Ok(jobs) = rx.await else {
            warning!("sync: evm: logs: failed to get jobs");
            return;
        };

        let jobs = jobs
            .evm_jobs()
            .log_jobs()
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        log!("sync: evm: logs: found {} jobs", jobs.len());

        for job in jobs {
            let channel = channel.clone();
            let handle = tokio::spawn(async move {
                let mut retries = 0;
                'job: loop {
                    if retries >= 10 {
                        warning!(
                            "sync: evm: logs: {}: too many retries, stopping job",
                            &job.name
                        );

                        return;
                    }

                    if let Err(error) = job.connect_evm().await {
                        warning!(
                          "sync: evm: logs: {}: failed to connect with provider with {}",
                          &job.name,
                          error
                      );

                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };

                    let mut stream = match build_stream(&job).await {
                        Ok(stream) => StreamNotifyClose::new(stream),
                        Err(error) => {
                            warning!(
                                "sync: evm: logs: {}: failed to build stream with {}",
                                &job.name,
                                error
                            );

                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(200))
                                .await;
                            continue;
                        }
                    };

                    channel
                        .send(Message::UpdateJob(job.id, JobStatus::Running));

                    log!("sync: evm: logs: {}: started listening", &job.name);
                    loop {
                        match stream.next().await {
                            Some(Some(log)) => {
                                handle_evm_log(&job, log, &channel).await
                            }
                            _ => {
                                warning!(
                                    "sync: evm: logs: {}: stream has ended, restarting provider",
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
                    Signal::RestartLogs => {
                        log!("sync: evm: logs: restarting jobs");
                        for handle in handles {
                            handle.abort();
                        }

                        continue 'logs;
                    }
                    _ => {}
                },
                Err(_) => {}
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub async fn handle_evm_log(
    job: &Arc<Job>,
    log: alloy::rpc::types::Log,
    channel: &Channel,
) {
    let Some(transaction) = log.transaction_hash else {
        warning!("sync: evm: logs: {}: found pending, skipping", &job.name);
        return;
    };

    let Some(block) = log.block_number else {
        warning!("sync: evm: logs: {}: found pending, skipping", &job.name);
        return;
    };

    let Some(log_index) = log.log_index else {
        warning!("sync: evm: logs: {}: found pending, skipping", &job.name);
        return;
    };

    if let Some(event) = &job.options.event {
        let _ = keccak256(event.as_bytes());
        if !matches!(log.topic0(), Some(_)) {
            warning!(
                "sync: evm: logs: {}: {}<{}>: topic0 does not match",
                &job.name,
                transaction,
                log_index
            );
            return;
        }
    } else if let Some(topic0) = &job.options.topic0 {
        let _ = topic0.parse::<B256>().unwrap();
        if !matches!(log.topic0(), Some(_)) {
            warning!("sync: evm: logs: {}: topic0 does not match", &job.name);
            return;
        }
    }

    log!(
        "sync: evm: logs: {}: found {}<{}> at {}",
        &job.name,
        transaction,
        log_index,
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

                    // Reconnect ws on every block retry
                    let Ok(client) = job.reconnect_evm().await else {
                        warning!(
                            "sync: evm: logs: {}: failed to connect to evm at await block handler",
                            &job.name
                        );
                        sleep(Duration::from_millis(1000)).await;
                        retries = retries + 1;
                        continue;
                    };

                    if let Ok(Some(block)) = client
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
            log_index
        )
    }
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
