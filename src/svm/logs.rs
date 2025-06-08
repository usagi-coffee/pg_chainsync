use pgrx::{log, warning};

use anyhow::{bail, ensure, Context};
use solana_client::rpc_config::{
    RpcTransactionLogsConfig, RpcTransactionLogsFilter,
};

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt, StreamNotifyClose};

use solana_client::rpc_response::{Response, RpcLogsResponse};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use bus::BusReader;

use crate::svm::transactions;
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

                    if let Err(error) = job.reconnect_svm_ws().await {
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
                                if let Err(error) =
                                    handle_svm_log(&job, log, &channel).await
                                {
                                    warning!(
                                      "sync: svm: logs: {}: failed to handle log with {}",
                                      &job.name,
                                      error
                                  );
                                }
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
) -> Result<(), anyhow::Error> {
    let Some(options) = &job.options.svm else {
        bail!("sync: svm: logs: {}: job options are not set", &job.name);
    };

    log!(
        "sync: svm: logs: {}: found {} at {}",
        &job.name,
        &log.value.signature,
        &log.context.slot
    );

    if options.transaction_handler.is_some()
        || options.instruction_handler.is_some()
    {
        transactions::handle_log(job, &log, channel).await?;
    }

    if let Some(_) = &options.log_handler {
        ensure!(
            channel.send(Message::SvmLog(log, job.clone())),
            "sync: svm: logs: {}: failed to send log",
            &job.name
        );
    }

    Ok(())
}

pub fn build_config(_: &SvmOptions) -> RpcTransactionLogsConfig {
    RpcTransactionLogsConfig {
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        }),
    }
}

pub fn build_filter(options: &SvmOptions) -> RpcTransactionLogsFilter {
    if let Some(mentions) = &options.mentions {
        return RpcTransactionLogsFilter::Mentions(
            mentions.iter().map(|m| m.to_string()).collect(),
        );
    }

    RpcTransactionLogsFilter::All
}

pub async fn build_stream<'a>(
    job: &'a Job,
) -> anyhow::Result<
    Pin<Box<dyn Stream<Item = Response<RpcLogsResponse>> + 'a + Send>>,
> {
    let options = job.options.svm.as_ref().expect("SVM options are not set");
    let filter = build_filter(options);
    let provider = job.connect_svm_ws().await.context("Invalid provider")?;
    let sub = provider
        .logs_subscribe(filter, build_config(options))
        .await?;
    Ok(sub.0)
}
