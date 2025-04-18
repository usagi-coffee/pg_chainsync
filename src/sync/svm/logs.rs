use pgrx::prelude::*;
use pgrx::{log, warning};

use anyhow::Context;
use solana_client::rpc_config::{
    RpcTransactionLogsConfig, RpcTransactionLogsFilter,
};

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::{sleep, sleep_until, timeout, Duration, Instant};
use tokio_stream::{Stream, StreamExt, StreamMap, StreamNotifyClose};

use solana_client::rpc_response::{Response, RpcLogsResponse};
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
                .log_jobs()
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>();

            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                match job.connect_svm_ws().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!("sync: svm: logs: {}: ws: {}", job.id, err);
                        continue 'sync;
                    }
                }

                match job.connect_svm_rpc().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!("sync: svm: logs: {}: rpc: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                match build_stream(&job).await {
                    Ok(stream) => {
                        log!("sync: svm: logs: {} started listening", job.id);
                        channel.send(Message::UpdateJob(
                            JobStatus::Running,
                            Arc::clone(job),
                        ));

                        map.insert(i, StreamNotifyClose::new(stream));
                    }
                    Err(err) => {
                        warning!("sync: svm: logs: {}: {}", job.id, err);
                        continue 'sync;
                    }
                }
            }

            log!("sync: svm: logs: started listening to {} jobs", jobs.len());

            let mut drain = false;
            loop {
                match signals.try_recv() {
                    Ok(signal) => {
                        if signal == Signal::RestartLogs {
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
                            warning!(
                                "sync: svm: logs: stream {} has ended, restarting providers",
                                job.id
                            );
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
    job: &Arc<Job>,
    log: Response<RpcLogsResponse>,
    channel: &Channel,
) {
    let number = log.context.slot;
    log!(
        "sync: svm: logs: {}: found {}",
        &job.name,
        &log.value.signature
    );

    // Await for block logic
    if matches!(job.options.await_block, Some(true)) {
        let (tx, rx) = oneshot::channel::<bool>();

        if channel.send(Message::CheckBlock(number, tx, Arc::clone(job))) {
            let found = rx.await;

            if found.unwrap() == false {
                // Retry finding block until available
                let mut retries = 0;
                loop {
                    if retries > 20 {
                        panic!(
                            "sync: svm: logs: too many retries to get the block..."
                        );
                    }

                    if let Ok(block) = job
                        .connect_svm_rpc()
                        .await
                        .unwrap()
                        .get_block_with_config(
                            number,
                            crate::sync::svm::blocks::build_config(
                                &job.options,
                            ),
                        )
                        .await
                    {
                        channel.send(Message::SvmBlock(block, Arc::clone(job)));
                        break;
                    }

                    log!(
                        "sync: svm: logs: could not find block {}, retrying",
                        number
                    );

                    sleep(Duration::from_millis(1000)).await;
                    retries = retries + 1;
                }
            }
        }
    }

    if !channel.send(Message::SvmLog(log, Arc::clone(job))) {
        warning!("sync: svm: logs: {}: failed to send", &job.name)
    }
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

pub fn handle_message(message: Message) {
    let Message::SvmLog(log, job) = message else {
        return;
    };

    handle_log_message(log, job)
}

pub fn handle_log_message(log: SolanaLog, job: Arc<Job>) {
    let number = log.context.slot;
    log!("sync: svm: logs: {}: adding {}", "sol", number);

    let handler = job
        .options
        .log_handler
        .as_ref()
        .expect("sync: svm: logs: missing handler");

    let id = job.id;

    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            log.call_handler(&handler, id)
                .expect("sync: svm: logs: failed to call the handler {}");
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            warning!("sync: svm: logs: failed to call handler for {}", number);
        })
        .catch_others(|e| {
            log!("{:?}", e);
            warning!("sync: svm: logs: handler failed to put {}", number);
        })
        .execute();
    });
}

pub fn build_config(_: &JobOptions) -> RpcTransactionLogsConfig {
    RpcTransactionLogsConfig {
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        }),
    }
}

pub fn build_filter(options: &JobOptions) -> RpcTransactionLogsFilter {
    if let Some(mentions) = &options.mentions {
        return RpcTransactionLogsFilter::Mentions(mentions.clone());
    }

    RpcTransactionLogsFilter::All
}

pub async fn build_stream<'a>(
    job: &'a Job,
) -> anyhow::Result<Pin<Box<dyn Stream<Item = Response<RpcLogsResponse>> + 'a>>>
{
    let filter = build_filter(&job.options);
    let provider = job.connect_svm_ws().await.context("Invalid provider")?;
    let sub = provider
        .logs_subscribe(filter, build_config(&job.options))
        .await?;
    Ok(sub.0)
}
