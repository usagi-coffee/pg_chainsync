use pgrx::{log, warning};

use anyhow::{Context, bail, ensure};
use solana_client::rpc_config::{
    RpcBlockConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
};
use solana_transaction_status_client_types::{
    TransactionDetails, UiTransactionEncoding,
};

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt, StreamNotifyClose};

use solana_client::rpc_response::{Response, RpcBlockUpdate};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use bus::BusReader;

use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

fn ingress_key(job: &Job) -> String {
    let rpc = job.options.rpc.as_deref().unwrap_or("<missing-rpc>");
    let Some(options) = &job.options.svm else {
        return format!("rpc={}:svm=none", rpc);
    };

    format!(
        "rpc={}|mentions={:?}|tx_details={:?}",
        rpc, options.mentions, options.transaction_details
    )
}

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'blocks: loop {
        let mut handles = vec![];

        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        channel.send(Message::Jobs(tx));

        let Ok(jobs) = rx.await else {
            warning!("sync: svm: blocks: failed to get jobs");
            return;
        };

        let jobs = jobs
            .svm_jobs()
            .block_jobs()
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        log!("sync: svm: blocks: found {} jobs", jobs.len());

        let mut groups: HashMap<String, Vec<Arc<Job>>> = HashMap::new();
        for job in jobs {
            groups.entry(ingress_key(&job)).or_default().push(job);
        }

        log!("sync: svm: blocks: shared ingress groups: {}", groups.len());

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
                            "sync: svm: blocks: {}: too many retries, stopping shared group",
                            key
                        );
                        return;
                    }

                    if let Err(error) = primary.connect_svm_ws().await {
                        warning!(
                            "sync: svm: blocks: {}: failed to connect ws with {}",
                            key,
                            error
                        );

                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };

                    if let Err(error) = primary.connect_svm_rpc().await {
                        warning!(
                            "sync: svm: blocks: {}: failed to connect rpc with {}",
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
                                "sync: svm: blocks: {}: failed to build shared stream with {}",
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

                    log!("sync: svm: blocks: {}: started shared listener", key);
                    loop {
                        match stream.next().await {
                            Some(Some(block)) => {
                                for job in &group_jobs {
                                    if let Err(error) = handle_block(
                                        job,
                                        block.clone(),
                                        &channel,
                                    )
                                    .await
                                    {
                                        warning!(
                                            "sync: svm: blocks: {}: {}: failed to handle shared block with {}",
                                            key,
                                            &job.name,
                                            error
                                        );
                                    }
                                }
                            }
                            _ => {
                                warning!(
                                    "sync: svm: blocks: {}: shared stream ended, restarting",
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
                    Signal::RestartBlocks => {
                        log!("sync: svm: blocks: restarting jobs");
                        for handle in handles {
                            handle.abort();
                        }

                        continue 'blocks;
                    }
                    _ => {}
                },
                Err(_) => {}
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub async fn handle_block(
    job: &Arc<Job>,
    block: Response<RpcBlockUpdate>,
    channel: &Channel,
) -> Result<(), anyhow::Error> {
    let Some(block) = block.value.block else {
        bail!("sync: svm: blocks: {}: block is empty", &job.name);
    };

    let Some(block_height) = block.block_height else {
        bail!("sync: svm: blocks: {}: block height is empty", &job.name);
    };

    log!("sync: svm: blocks: {}: found {}", &job.name, block_height);
    ensure!(
        channel.send(Message::SvmBlock(block, job.clone())),
        "sync: svm: blocks: {}: failed to send",
        &job.name
    );

    Ok(())
}

pub fn build_filter(options: &SvmOptions) -> RpcBlockSubscribeFilter {
    if let Some(mentions) = &options.mentions {
        return RpcBlockSubscribeFilter::MentionsAccountOrProgram(
            mentions[0].to_string(),
        );
    }

    RpcBlockSubscribeFilter::All
}

pub fn build_config(options: &SvmOptions) -> RpcBlockConfig {
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

pub fn build_subscribe_config(options: &SvmOptions) -> RpcBlockSubscribeConfig {
    let mut config = RpcBlockSubscribeConfig {
        encoding: Some(UiTransactionEncoding::Base58),
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
) -> anyhow::Result<
    Pin<Box<dyn Stream<Item = Response<RpcBlockUpdate>> + 'a + Send>>,
> {
    let options = job.options.svm.as_ref().expect("SVM options are required");
    let filter = build_filter(options);
    let provider = job.connect_svm_ws().await.context("Invalid provider")?;
    let sub = provider
        .block_subscribe(filter, Some(build_subscribe_config(options)))
        .await?;

    Ok(sub.0)
}
