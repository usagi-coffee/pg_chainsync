use pgrx::{log, warning};

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, ensure};

use alloy::core::primitives::{Address, B256};
use alloy::primitives::keccak256;
use alloy::providers::Provider;
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::{BlockNumberOrTag, Filter};

use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::{StreamExt, StreamNotifyClose};

use bus::BusReader;

use crate::channel::Channel;
use crate::evm::blocks::try_block;
use crate::types::*;

fn ingress_key(handler: &HandlerRuntime) -> String {
    let ws = handler.options.ws.as_deref().unwrap_or("<missing-ws>");
    let Some(options) = &handler.options.evm else {
        return format!("ws={}:evm=none", ws);
    };

    format!(
        "ws={}|address={:?}|event={:?}|t0={:?}|t1={:?}|t2={:?}|t3={:?}|from={:?}|to={:?}",
        ws,
        options.address,
        options.event,
        options.topic0,
        options.topic1,
        options.topic2,
        options.topic3,
        options.from_block,
        options.to_block
    )
}

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'logs: loop {
        let mut handles = vec![];

        let (tx, rx) = oneshot::channel::<Vec<HandlerRuntime>>();
        channel.send(Message::Handlers(tx));

        let Ok(handlers) = rx.await else {
            warning!("sync: ingress: evm:logs: failed to load route table");
            return;
        };

        let handlers = handlers
            .evm_handlers()
            .log_handlers()
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        let mut groups: HashMap<String, Vec<Arc<HandlerRuntime>>> = HashMap::new();
        for handler in handlers {
            groups.entry(ingress_key(&handler)).or_default().push(handler);
        }

        for (key, group_handlers) in groups {
            let Some(primary) = group_handlers.first().cloned() else {
                continue;
            };
            let channel = channel.clone();
            let handle = tokio::spawn(async move {
                let mut retries = 0;
                'group: loop {
                    if retries >= 10 {
                        warning!(
                            "sync: ingress: evm:logs: {}: too many retries, stopping lane",
                            key
                        );

                        return;
                    }

                    if let Err(error) = primary.connect_evm().await {
                        warning!(
                            "sync: ingress: evm:logs: {}: provider connect failed: {}",
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
                                "sync: ingress: evm:logs: {}: stream build failed: {}",
                                key,
                                error
                            );

                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(200))
                                .await;
                            continue;
                        }
                    };

                    log!("sync: ingress: evm:logs: {}: lane online", key);
                    loop {
                        match stream.next().await {
                            Some(Some(event_log)) => {
                                for handler in &group_handlers {
                                    if let Err(error) = handle_evm_log(
                                        handler,
                                        event_log.clone(),
                                        &channel,
                                    )
                                    .await
                                    {
                                        warning!(
                                            "sync: ingress: evm:logs: {}: route={} dispatch failed: {}",
                                            key,
                                            &handler.name,
                                            error
                                        );
                                    }
                                }

                                retries = 0;
                            }
                            _ => {
                                warning!(
                                    "sync: ingress: evm:logs: {}: stream ended, reconnecting",
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
                    Signal::RestartLogs => {
                        log!("sync: ingress: evm:logs: reload signal received, restarting lane");
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
    handler: &Arc<HandlerRuntime>,
    log: alloy::rpc::types::Log,
    channel: &Channel,
) -> Result<(), anyhow::Error> {
    let Some(options) = &handler.options.evm else {
        bail!("sync: ingress: evm:logs: {}: missing evm options", &handler.name);
    };

    let Some(transaction) = log.transaction_hash else {
        warning!("sync: ingress: evm:logs: {}: pending tx, skipping", &handler.name);
        return Ok(());
    };

    let Some(block) = log.block_number else {
        warning!("sync: ingress: evm:logs: {}: pending block, skipping", &handler.name);
        return Ok(());
    };

    let Some(log_index) = log.log_index else {
        warning!("sync: ingress: evm:logs: {}: pending log index, skipping", &handler.name);
        return Ok(());
    };

    if let Some(event) = &options.event
        && let _hash = keccak256(event.as_bytes())
        && !matches!(log.topic0(), Some(_hash))
    {
        warning!(
            "sync: ingress: evm:logs: {}: {}<{}>: topic0 mismatch",
            &handler.name,
            transaction,
            log_index,
        );

        return Ok(());
    }

    if let Some(topic0) = &options.topic0
        && let Ok(_hash) = topic0.parse::<B256>()
        && !matches!(log.topic0(), Some(_hash))
    {
        warning!("sync: ingress: evm:logs: {}: topic0 mismatch", &handler.name);
        return Ok(());
    }

    // Await for block logic
    if matches!(options.await_block, Some(true)) {
        match try_block(block, &handler).await {
            Ok(block) => {
                let inner = block.0.inner;
                ensure!(
                    channel.send(Message::EvmBlock(
                        inner.into_header(),
                        handler.clone(),
                    )),
                    "sync: ingress: evm:logs: {}: enqueue awaited block failed for {}<{}>",
                    &handler.name,
                    transaction,
                    log_index,
                );
            }
            Err(error) => {
                bail!(
                    "sync: ingress: evm:logs: {}: await_block fetch {} failed: {}",
                    &handler.name,
                    block,
                    error
                );
            }
        };
    }

    ensure!(
        channel.send(Message::EvmLog(log, handler.clone())),
        "sync: ingress: evm:logs: {}: enqueue {}<{}> failed",
        &handler.name,
        transaction,
        log_index
    );

    Ok(())
}

pub fn build_filter(options: &EvmOptions, block: u64) -> Filter {
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
    handler: &HandlerRuntime,
) -> anyhow::Result<SubscriptionStream<alloy::rpc::types::Log>> {
    let ws = handler.connect_evm().await.unwrap();
    let block = ws
        .get_block_number()
        .await
        .expect("failed to retrieve latest block") as u64;

    let filter = build_filter(
        handler.options.evm.as_ref().expect("evm options to be set"),
        block,
    );

    let sub = handler
        .connect_evm()
        .await
        .unwrap()
        .subscribe_logs(&filter)
        .await?;
    Ok(sub.into_stream())
}
