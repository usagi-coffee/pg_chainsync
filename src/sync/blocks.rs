use ethers::providers::SubscriptionStream;
use pgrx::prelude::*;
use pgrx::{log, warning};

use anyhow::{Context, Result};

use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{sleep_until, timeout, Duration, Instant};
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose};

use ethers::providers::Middleware;

use bus::BusReader;

use ethers::types::{Chain, H256, U64};

use crate::types::Job;

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
                if job.kind != JobKind::Blocks || job.oneshot {
                    continue;
                }

                match job.connect().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!("sync: blocks: {}: ws: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                if job.kind != JobKind::Blocks || job.oneshot {
                    continue;
                }

                if job.ws.is_none() {
                    continue 'sync;
                }

                match build_stream(&job).await {
                    Ok(stream) => {
                        log!("sync: blocks: {} started listening", job.id);
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
    let Message::Block(chain, block, callback, job_id) = message else {
        return;
    };

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

pub async fn build_stream(
    job: &Job,
) -> Result<
    SubscriptionStream<'_, ethers::providers::Ws, ethers::types::Block<H256>>,
> {
    let provider = job.ws.as_ref().context("Invalid provider")?;
    Ok(provider.subscribe_blocks().await?)
}
