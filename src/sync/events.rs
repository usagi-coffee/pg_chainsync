use pgx::log;
use pgx::prelude::*;

use std::sync::Arc;

use tokio_stream::{pending, StreamMap};

use ethers::prelude::*;

use super::{wait_for_messages, MessageSender};
use crate::types::*;

pub async fn listen(jobs: Arc<Vec<Job>>, send_message: MessageSender) {
    let mut map = StreamMap::new();

    for i in 0..jobs.len() {
        let job = &jobs[i];

        if job.job_type != JobType::Events {
            continue;
        }

        let options = job.options.as_ref().unwrap();

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

        if let Some(to_block) = &options.to_block {
            filter = filter.to_block(to_block.clone());
        }

        if let Some(from_block) = &options.from_block {
            filter = filter.from_block(from_block.clone());
        }

        let provider = job.ws.as_ref().unwrap();
        let stream = provider.subscribe_logs(&filter).await.unwrap();

        map.insert(i, stream);
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

        let transaction = log.transaction_hash.unwrap();
        let index = log.log_index.unwrap();

        log!(
            "sync: events: {}: found {}<{}> at {}",
            job.chain,
            transaction,
            index,
            log.block_number.unwrap()
        );

        match send_message
            .try_send((job.chain, Message::Event(log, job.callback.clone())))
        {
            Ok(()) => {}
            Err(_) => {
                warning!(
                    "sync: events: {}: failed to send {}<{}>",
                    job.chain,
                    transaction,
                    index
                )
            }
        }
    }

    // Wait until queue gets processed before exiting
    log!("sync: events: waiting for consumer to finish");
    wait_for_messages(send_message).await;
}
