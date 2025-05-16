use pgrx::{log, warning};

use anyhow::Context;
use solana_client::rpc_config::{
    RpcTransactionLogsConfig, RpcTransactionLogsFilter,
};

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tokio_stream::{Stream, StreamExt, StreamNotifyClose};

use solana_client::rpc_response::{Response, RpcLogsResponse};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use bus::BusReader;

use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'logs: loop {
        let mut handles = vec![];

        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        channel.send(Message::Jobs(tx));

        let Ok(jobs) = rx.await else {
            warning!("sync: svm: logs: failed to get jobs");
            return;
        };

        let jobs = jobs
            .svm_jobs()
            .log_jobs()
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        log!("sync: svm: logs: found {} jobs", jobs.len());

        for job in jobs {
            let channel = channel.clone();
            let handle = tokio::spawn(async move {
                let mut retries = 0;
                'job: loop {
                    if retries >= 10 {
                        warning!(
                          "sync: svm: logs: {}: too many retries, stopping job",
                          &job.name
                      );

                        return;
                    }

                    if let Err(error) = job.connect_svm_ws().await {
                        warning!(
                            "sync: svm: logs: {}: failed to connect with ws with {}",
                            &job.name,
                            error
                        );

                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };

                    if let Err(error) = job.connect_svm_rpc().await {
                        warning!(
                            "sync: svm: logs: {}: failed to connect with rpc with {}",
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
                                "sync: svm: logs: {}: failed to build stream with {}",
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

                    log!("sync: svm: logs: {}: started listening", &job.name);
                    loop {
                        match stream.next().await {
                            Some(Some(log)) => {
                                handle_svm_log(&job, log, &channel).await
                            }
                            _ => {
                                warning!(
                                  "sync: svm: logs: {}: stream has ended, restarting provider",
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
                        log!("sync: svm: logs: restarting jobs");
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

pub async fn handle_svm_log(
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
                            crate::svm::blocks::build_config(&job.options),
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
) -> anyhow::Result<
    Pin<Box<dyn Stream<Item = Response<RpcLogsResponse>> + 'a + Send>>,
> {
    let filter = build_filter(&job.options);
    let provider = job.connect_svm_ws().await.context("Invalid provider")?;
    let sub = provider
        .logs_subscribe(filter, build_config(&job.options))
        .await?;
    Ok(sub.0)
}
