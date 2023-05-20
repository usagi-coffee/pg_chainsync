use pgx::log;
use pgx::prelude::*;

use std::sync::Arc;

use tokio::sync::oneshot;
use tokio_stream::{pending, StreamMap, StreamNotifyClose};

use ethers::prelude::*;

use crate::channel::Channel;
use crate::types::*;

pub async fn listen(jobs: Arc<Vec<Job>>, channel: Arc<Channel>) {
    let mut map = StreamMap::new();

    for i in 0..jobs.len() {
        let job = &jobs[i];

        if job.kind != JobKind::Events {
            continue;
        }

        let options = job.options.as_ref().unwrap();
        let filter = build_filter(&options);

        let provider = job.ws.as_ref().unwrap();
        let stream = provider.subscribe_logs(&filter).await.unwrap();

        map.insert(i, StreamNotifyClose::new(stream));
    }

    if map.is_empty() {
        log!("sync: events: no jobs for events");
        pending::<()>().next().await;
        unreachable!();
    }

    log!("sync: events: started listening");

    while let Some(tick) = map.next().await {
        let (i, log) = tick;
        let job = &jobs[i];

        if log.is_none() {
            println!("sync: events: stream {} has ended", job.id);
            continue;
        }

        handle_log(job, log.unwrap(), &channel).await;
    }

    // Wait until queue gets processed before exiting
    log!("sync: events: waiting for consumer to finish");
    channel.wait_for_messages().await;
}

pub async fn handle_log(job: &Job, log: ethers::types::Log, channel: &Channel) {
    let chain = &job.chain;

    let block = log.block_number.unwrap();
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
                check_block,
                block_handler,
            } = &options;

            let (tx, rx) = oneshot::channel::<bool>();

            if channel.send(Message::CheckBlock(
                *chain,
                block.as_u64(),
                check_block.as_ref().unwrap().clone(),
                tx,
            )) {
                let found = rx.await;
                if found.is_ok() && found.unwrap() == false {
                    channel.send(Message::Block(
                        *chain,
                        job.ws
                            .as_ref()
                            .unwrap()
                            .get_block(block)
                            .await
                            .unwrap()
                            .unwrap(),
                        block_handler.as_ref().unwrap().clone(),
                    ));
                }
            }
        }
    }

    if !channel.send(Message::Event(*chain, log, job.callback.clone())) {
        warning!(
            "sync: events: {}: failed to send {}<{}>",
            job.chain,
            transaction,
            index
        )
    }
}

use crate::query::PgHandler;
use pgx::bgworkers::BackgroundWorker;

pub fn handle_message(message: &Message) {
    let Message::Event(chain, log, callback) = message else { return; };

    let transaction = log.transaction_hash.unwrap();
    let index = log.log_index.unwrap();

    log!("sync: events: {}: adding {}<{}>", chain, transaction, index);

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            log.call_handler(&chain, &callback)
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

fn build_filter(options: &JobOptions) -> Filter {
    let mut filter = Filter::new().from_block(BlockNumber::Latest);

    if let Some(address) = &options.address {
        filter = filter.address(address.parse::<Address>().unwrap());
    }

    if let Some(event) = &options.event {
        filter = filter.event(event);
    }

    if let Some(topic0) = &options.topic0 {
        filter = filter.topic0(topic0.parse::<H256>().unwrap());
    }

    if let Some(from_block) = &options.from_block {
        filter = filter.from_block(from_block.clone());
    }

    if let Some(_) = &options.to_block {
        todo!("to_block implies oneshot/get_logs");
    }

    filter
}
