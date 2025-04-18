use pgrx::prelude::*;
use pgrx::{log, warning};

use anyhow::Context;
use solana_client::rpc_config::{
    RpcBlockConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
};
use solana_transaction_status_client_types::TransactionDetails;

use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{sleep_until, timeout, Duration, Instant};
use tokio_stream::{Stream, StreamExt, StreamMap, StreamNotifyClose};

use solana_client::rpc_response::{Response, RpcBlockUpdate};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use bus::BusReader;

use crate::types::svm::*;
use crate::types::Job;

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
                .svm_jobs()
                .block_jobs()
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>();

            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                match job.connect_svm_ws().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!("sync: svm: blocks: {}: ws: {}", job.id, err);
                        continue 'sync;
                    }
                }

                match job.connect_svm_rpc().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!("sync: svm: blocks: {}: rpc: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                match build_stream(&job).await {
                    Ok(stream) => {
                        log!("sync: svm: blocks: {} started listening", job.id);
                        channel.send(Message::UpdateJob(
                            JobStatus::Running,
                            Arc::clone(job),
                        ));

                        map.insert(i, StreamNotifyClose::new(stream));
                    }
                    Err(err) => {
                        warning!("sync: svm: blocks: {}: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            log!(
                "sync: svm: blocks: started listening to {} jobs",
                jobs.len()
            );

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
                                "sync: svm: blocks: stream {} has ended, restarting providers",
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
    job: &Arc<Job>,
    block: Response<RpcBlockUpdate>,
    channel: &Channel,
) {
    let block = block.value.block.unwrap();
    log!(
        "sync: svm: blocks: {}: found {}",
        &job.name,
        block.block_height.as_ref().unwrap()
    );

    if !channel.send(Message::SvmBlock(block, Arc::clone(job))) {
        warning!("sync: svm: blocks: {}: failed to send", &job.name)
    }
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: Message) {
    log!("sync: svm: blocks: handling message");
    let Message::SvmBlock(block, job) = message else {
        return;
    };
    log!("sync: svm: blocks: handling message next");

    handle_block_message(block, job)
}

pub fn handle_block_message(block: SvmBlock, job: Arc<Job>) {
    let number = block.block_height.unwrap();
    log!("sync: svm: blocks: {}: adding {}", "sol", number);

    let handler = job
        .options
        .block_handler
        .as_ref()
        .expect("sync: svm: blocks: missing handler");

    let id = job.id;

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            block
                .call_handler(&handler, id)
                .expect("sync: svm: blocks: failed to call the handler {}");
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            warning!(
                "sync: svm: blocks: failed to call handler for {}",
                number
            );
        })
        .catch_others(|e| {
            log!("{:?}", e);
            warning!("sync: svm: blocks: handler failed to put {}", number);
        })
        .execute();
    });
}

pub fn build_filter(options: &JobOptions) -> RpcBlockSubscribeFilter {
    if let Some(mentions) = &options.mentions {
        return RpcBlockSubscribeFilter::MentionsAccountOrProgram(
            mentions[0].clone(),
        );
    }

    RpcBlockSubscribeFilter::All
}

pub fn build_config(options: &JobOptions) -> RpcBlockConfig {
    let mut config = RpcBlockConfig {
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        }),
        transaction_details: Some(TransactionDetails::None),
        max_supported_transaction_version: Some(0),
        ..Default::default()
    };

    config.transaction_details = options.transaction_details.clone();

    config
}

pub fn build_subscribe_config(options: &JobOptions) -> RpcBlockSubscribeConfig {
    let mut config = RpcBlockSubscribeConfig {
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        }),
        transaction_details: Some(TransactionDetails::None),
        max_supported_transaction_version: Some(0),
        ..Default::default()
    };

    config.transaction_details = options.transaction_details.clone();

    config
}

pub async fn build_stream<'a>(
    job: &'a Job,
) -> anyhow::Result<Pin<Box<dyn Stream<Item = Response<RpcBlockUpdate>> + 'a>>>
{
    let filter = build_filter(&job.options);
    let provider = job.connect_svm_ws().await.context("Invalid provider")?;
    let sub = provider
        .block_subscribe(filter, Some(build_subscribe_config(&job.options)))
        .await?;

    Ok(sub.0)
}
