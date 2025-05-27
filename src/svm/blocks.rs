use pgrx::{log, warning};

use anyhow::{bail, ensure, Context};
use solana_client::rpc_config::{
    RpcBlockConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
};
use solana_transaction_status_client_types::{
    TransactionDetails, UiTransactionEncoding,
};

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

        for job in jobs {
            let channel = channel.clone();
            let handle = tokio::spawn(async move {
                let mut retries = 0;
                'job: loop {
                    if retries >= 10 {
                        warning!(
                        "sync: svm: blocks: {}: too many retries, stopping job",
                        &job.name
                    );
                        return;
                    }

                    if let Err(error) = job.connect_svm_ws().await {
                        warning!(
                            "sync: svm: blocks: {}: failed to connect with provider with {}",
                            &job.name,
                            error
                        );

                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };

                    if let Err(error) = job.connect_svm_rpc().await {
                        warning!(
                            "sync: svm: blocks: {}: failed to connect with provider with {}",
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
                                "sync: svm: blocks: {}: failed to build stream with {}",
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

                    log!("sync: svm: blocks: {}: started listening", &job.name);
                    loop {
                        match stream.next().await {
                            Some(Some(block)) => {
                                if let Err(error) =
                                    handle_block(&job, block, &channel).await
                                {
                                    warning!(
                                        "sync: svm: blocks: {}: failed to handle block with {}",
                                        &job.name,
                                        error
                                    );
                                }
                            }
                            _ => {
                                warning!(
                                    "sync: svm: blocks: {}: stream has ended, restarting provider",
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
