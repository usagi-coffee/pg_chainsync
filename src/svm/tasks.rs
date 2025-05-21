use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure};

use pgrx::{log, warning, JsonB};

use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

use tokio::sync::oneshot;
use tokio::time::{sleep_until, Duration, Instant};

use super::transactions;
use super::{blocks, SvmTransaction};
use crate::channel::Channel;
use crate::types::*;
use crate::worker::SVM_TASKS;

pub async fn handle_tasks(channel: Arc<Channel>) {
    loop {
        let task: Option<i64> = { SVM_TASKS.exclusive().pop() };

        if let Some(task) = task {
            log!("sync: svm: tasks: got task {}", task);

            // FIXME: wait some time for commit when adding tasks
            sleep_until(Instant::now() + Duration::from_millis(250)).await;

            let (tx, rx) = oneshot::channel::<Option<Job>>();
            channel.send(Message::Job(task, tx));

            let Ok(job) = rx.await else {
                warning!(
                    "sync: svm: tasks: failed to fetch job for task {}",
                    task
                );
                continue;
            };

            let Some(mut job) = job else {
                warning!(
                    "sync: svm: tasks: failed to find job for task {}",
                    task
                );
                continue;
            };

            if job.status == String::from(JobStatus::Running) {
                log!("sync: svm: tasks: {}: job is already running", &job.name);
                if let Some(upcoming) = job.options.next_cron() {
                    log!(
                        "sync: svm: tasks: {}: next at {}",
                        &job.name,
                        upcoming
                    );
                }
                continue;
            }

            if let Some(setup_handler) = job.options.setup_handler.as_ref() {
                let (tx, rx) = oneshot::channel::<Option<JsonB>>();
                channel.send(Message::JsonHandler(
                    task,
                    setup_handler.to_owned(),
                    tx,
                ));

                // Update options
                match rx.await {
                    Ok(Some(json)) => match serde_json::from_value(json.0) {
                        Ok(options) => job.options = options,
                        Err(error) => {
                            warning!("sync: svm: tasks: {}: failed to parse return handler options jsonb with {}", &job.name, &error);
                            continue;
                        }
                    },
                    Err(error) => {
                        warning!(
                        "sync: svm: tasks: {}: setup handler failed with {}",
                        &job.name,
                        error
                    );
                        continue;
                    }
                    _ => {
                        warning!("sync: svm: tasks: {}: setup handler did not return options jsonb", &job.name);
                        continue;
                    }
                }
            }

            if job.options.rpc.is_none() {
                warning!(
                    "sync: svm: tasks: {}: no rpc was provided",
                    &job.name
                );
                continue;
            };

            let job = Arc::new(job);

            let task = {
                if job.options.is_block_job() {
                    handle_blocks_task(Arc::clone(&job), &channel).await
                } else if job.options.is_log_job() {
                    Err(anyhow!("log tasks are not supported yet"))
                } else if job.options.is_transaction_job() {
                    handle_transactions_task(Arc::clone(&job), &channel).await
                } else {
                    Err(anyhow!("unknown task"))
                }
            };

            match task {
                Ok(()) => {
                    log!("sync: svm: tasks: {}: task completed", &job.name);

                    if let Some(success_handler) = &job.options.success_handler
                    {
                        let (tx, rx) = oneshot::channel::<bool>();
                        channel.send(Message::Handler(
                            job.id,
                            success_handler.to_owned(),
                            tx,
                        ));

                        let _ = rx.await;
                    }
                }
                Err(error) => {
                    warning!(
                        "sync: svm: tasks: {}: task failed with {}",
                        &job.name,
                        error
                    );

                    if let Some(failure_handler) = &job.options.failure_handler
                    {
                        let (tx, rx) = oneshot::channel::<bool>();
                        channel.send(Message::Handler(
                            job.id,
                            failure_handler.to_owned(),
                            tx,
                        ));
                        let _ = rx.await;
                    }
                }
            };

            if let Some(upcoming) = job.options.next_cron() {
                log!("sync: svm: tasks: {}: next at {}", &job.name, upcoming);
            }
        }

        sleep_until(Instant::now() + Duration::from_millis(250)).await;
    }
}

async fn handle_blocks_task(
    job: Arc<Job>,
    channel: &Arc<Channel>,
) -> Result<(), anyhow::Error> {
    let options = &job.options;

    let rpc = job.reconnect_svm_rpc().await;

    let Some(from) = options.from_slot else {
        bail!("from_slot is required for block task");
    };

    let to = {
        if let Some(to_slot) = options.to_slot {
            to_slot
        } else {
            rpc.get_slot().await?
        }
    };

    let mut retries = 0;
    'blocks: loop {
        ensure!(
            retries < 10,
            "failed to get block after 10 retries, aborting..."
        );

        for i in from..to {
            match rpc
                .get_block_with_config(i, blocks::build_config(&options))
                .await
            {
                Ok(block) => {
                    channel.send(Message::SvmBlock(block, Arc::clone(&job)));
                }
                Err(error) => {
                    warning!(
                      "sync: svm: blocks: {}: failed to get block with {}, reconnecting...",
                      &job.name,
                      error
                    );
                    retries += 1;

                    sleep_until(Instant::now() + Duration::from_millis(250))
                        .await;
                    continue 'blocks;
                }
            }
        }

        break;
    }

    Ok(())
}

async fn handle_transactions_task(
    job: Arc<Job>,
    channel: &Arc<Channel>,
) -> Result<(), anyhow::Error> {
    let rpc = job.reconnect_svm_rpc().await;

    let Some(mentions) = job.options.mentions.as_ref() else {
        bail!("mentions field is required for transaction task");
    };

    let address = Pubkey::from_str(mentions[0].as_str())?;

    let mut before: Option<Signature> = None;
    let mut until: Option<Signature> = None;

    if let Some(before_signature) = job.options.before.as_ref() {
        before = Some(Signature::from_str(before_signature)?);
    }

    if let Some(until_signature) = job.options.until.as_ref() {
        until = Some(Signature::from_str(until_signature)?);
    }

    let mut signatures = Vec::new();

    'fill: loop {
        match rpc.get_signatures_for_address_with_config(
          &address,
          solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
              before,
              until,
              limit: Some(1000),
              commitment: Some(solana_sdk::commitment_config::CommitmentConfig {
                  commitment: CommitmentLevel::Finalized,
              }),
          },
      ).await {
        Ok(sigs) => {
          if sigs.is_empty() {
            break 'fill;
          }

          before = Signature::from_str(sigs[sigs.len() - 1].signature.as_str())?.into();
          signatures.extend(sigs);
        },
        Err(_) => {
          warning!("sync: svm: tasks: failed to get signatures");
          break 'fill;
        }
      }
    }

    let mut retries = 0;
    'transactions: loop {
        ensure!(
            retries < 10,
            "failed to get transaction after 10 retries, aborting..."
        );

        for signature in &signatures {
            let signature = Signature::from_str(signature.signature.as_str())?;

            let tx = match rpc
                .get_transaction_with_config(
                    &signature,
                    transactions::build_config(&job.options),
                )
                .await
            {
                Ok(tx) => tx,
                Err(error) => {
                    warning!(
                        "sync: svm: tasks: {}: {}: failed to get transaction with {}, retrying",
                        &job.name,
                        &signature,
                        error
                    );

                    retries += 1;
                    sleep_until(Instant::now() + Duration::from_millis(250))
                        .await;
                    continue 'transactions;
                }
            };

            retries = 0;

            match tx.try_into() as Result<SvmTransaction, anyhow::Error> {
                Ok(tx) => {
                    if !tx.failed {
                        channel.send(Message::SvmTransaction(tx, job.clone()));
                    }
                }
                Err(error) => {
                    warning!(
                        "sync: svm: tasks: {}: {}: transaction validation failed with {}",
                        &job.name,
                        &signature,
                        error
                    );
                }
            }
        }

        break;
    }

    Ok(())
}
