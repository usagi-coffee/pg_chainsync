use pgrx::log;
use pgrx::prelude::*;

use std::sync::Arc;

use ethers::providers::Middleware;

use tokio_stream::{pending, StreamExt, StreamMap, StreamNotifyClose};

use ethers::types::{Chain, H256, U64};

use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

pub async fn listen(jobs: Arc<Vec<Job>>, channel: Arc<Channel>) {
    let mut map = StreamMap::new();

    for i in 0..jobs.len() {
        let job = &jobs[i];

        if job.kind != JobKind::Blocks || job.oneshot {
            continue;
        }

        let stream = job.ws.as_ref().unwrap().subscribe_blocks().await.unwrap();

        log!("sync: blocks: {} started listening", job.id);
        map.insert(i, StreamNotifyClose::new(stream));
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

        if block.is_none() {
            println!("sync: blocks: stream {} has ended", job.id);
            continue;
        }

        handle_block(job, block.unwrap(), &channel).await;
    }

    // Wait until queue gets processed before exiting
    log!("sync: blocks: waiting for consumer to finish");
    channel.wait_for_messages().await;
}

pub async fn handle_block(
    job: &Job,
    block: ethers::types::Block<H256>,
    channel: &Channel,
) {
    let chain = &job.chain;

    let number = block.number.unwrap_or(U64::from(0));
    log!("sync: blocks: {}: found {}", chain, number);

    channel.send(Message::Block(
        *chain,
        block,
        job.callback.clone(),
        Some(job.id),
    ));
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: &Message) {
    let Message::Block(chain, block, callback, job_id) = message else { return; };

    let number = block.number.unwrap_or_default();
    log!("sync: blocks: {}: adding {}", chain, number);

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            block
                .call_handler(&chain, callback, &job_id.unwrap_or(-1))
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
