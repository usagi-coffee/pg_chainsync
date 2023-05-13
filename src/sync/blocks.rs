use pgx::log;
use pgx::prelude::*;

use std::sync::Arc;

use tokio_stream::{pending, StreamMap};

use ethers::prelude::*;

use super::{wait_for_messages, MessageSender};
use crate::types::*;

pub async fn listen(jobs: Arc<Vec<Job>>, channel: MessageSender) {
    let mut map = StreamMap::new();

    for i in 0..jobs.len() {
        let job = &jobs[i];

        if job.job_type != JobType::Blocks {
            continue;
        }

        let stream = job.ws.as_ref().unwrap().subscribe_blocks().await.unwrap();

        map.insert(i, stream);
    }

    if map.is_empty() {
        log!("sync: blocks: no jobs for blocks");
        pending::<()>().next().await;
        unreachable!();
    }

    log!("sync: blocks: started listening");

    while let Some(tick) = map.next().await {
        let (i, block) = tick;
        let job = &jobs[i];
        let chain = &job.chain;

        let number = block.number.unwrap_or(U64::from(0));
        log!("sync: blocks: {}: found {}", chain, number);

        match channel.try_send(Message::Block(
            *chain,
            block,
            job.callback.clone(),
        )) {
            Ok(()) => {}
            Err(_) => {
                warning!("sync: blocks: {}: failed to send {}", chain, number)
            }
        }
    }

    // Wait until queue gets processed before exiting
    log!("sync: blocks: waiting for consumer to finish");
    wait_for_messages(channel).await;
}

use super::call_block_handler;
use pgx::bgworkers::BackgroundWorker;

pub fn handle_message(message: &Message) {
    let Message::Block(chain, block, callback) = message else { return; };

    let number = block.number.unwrap_or_default();
    log!("sync: blocks: {}: adding {}", chain, number);

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            call_block_handler(callback, &block)
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

pub fn check_one(chain: &Chain, number: &u64, callback: &String) -> bool {
    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            let found = Spi::get_one_with_args::<i64>(
                format!("SELECT {}($1, $2)", callback).as_str(),
                vec![
                    (
                        PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                        (*chain as i64).into_datum(),
                    ),
                    (
                        PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                        (*number as i64).into_datum(),
                    ),
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
