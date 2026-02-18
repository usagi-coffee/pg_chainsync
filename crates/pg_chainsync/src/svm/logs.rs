use pgrx::{log, warning};

use anyhow::{Context, bail, ensure};
use solana_client::rpc_config::{
    RpcTransactionLogsConfig, RpcTransactionLogsFilter,
};

use std::collections::HashMap;
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

fn ingress_key(job: &Job) -> String {
    let rpc = job.options.rpc.as_deref().unwrap_or("<missing-rpc>");
    let Some(options) = &job.options.svm else {
        return format!("rpc={}:svm=none", rpc);
    };

    format!("rpc={}|mentions={:?}", rpc, options.mentions)
}

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'logs: loop {
        let mut handles = vec![];

        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        channel.send(Message::Jobs(tx));

        let Ok(jobs) = rx.await else {
            warning!("sync: svm: logs: failed to get handlers");
            return;
        };

        let jobs = jobs
            .svm_jobs()
            .log_jobs()
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        log!("sync: svm: logs: found {} handlers", jobs.len());

        let mut groups: HashMap<String, Vec<Arc<Job>>> = HashMap::new();
        for job in jobs {
            groups.entry(ingress_key(&job)).or_default().push(job);
        }

        log!("sync: svm: logs: shared ingress groups: {}", groups.len());

        for (key, group_jobs) in groups {
            let Some(primary) = group_jobs.first().cloned() else {
                continue;
            };
            let channel = channel.clone();
            let handle = tokio::spawn(async move {
                let mut retries = 0;
                'group: loop {
                    if retries >= 10 {
                        warning!(
                            "sync: svm: logs: {}: too many retries, stopping shared group",
                            key
                        );

                        return;
                    }

                    if let Err(error) = primary.reconnect_svm_ws().await {
                        warning!(
                            "sync: svm: logs: {}: failed to connect ws with {}",
                            key,
                            error
                        );

                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };

                    if let Err(error) = primary.connect_svm_rpc().await {
                        warning!(
                            "sync: svm: logs: {}: failed to connect rpc with {}",
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
                                "sync: svm: logs: {}: failed to build shared stream with {}",
                                key,
                                error
                            );

                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(200))
                                .await;
                            continue;
                        }
                    };

                    for job in &group_jobs {
                        channel.send(Message::UpdateJob(
                            job.id,
                            JobStatus::Running,
                        ));
                    }

                    log!("sync: svm: logs: {}: started shared listener", key);
                    loop {
                        match stream.next().await {
                            Some(Some(svm_log)) => {
                                for job in &group_jobs {
                                    if let Err(error) = handle_svm_log(
                                        job,
                                        svm_log.clone(),
                                        &channel,
                                    )
                                    .await
                                    {
                                        warning!(
                                            "sync: svm: logs: {}: {}: failed to handle shared log with {}",
                                            key,
                                            &job.name,
                                            error
                                        );
                                    }
                                }
                            }
                            _ => {
                                warning!(
                                    "sync: svm: logs: {}: shared stream ended, restarting",
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
                        log!("sync: svm: logs: restarting handlers");
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
