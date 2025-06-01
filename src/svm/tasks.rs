use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure};

use pgrx::{log, warning, JsonB};

use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;

use tokio::sync::{oneshot, Semaphore};
use tokio::time::{sleep_until, Duration, Instant};

use indexmap::IndexSet;

use super::transactions;
use super::{blocks, SvmTransaction};
use crate::channel::Channel;
use crate::types::*;
use crate::worker::{SVM_RPC_PERMITS, SVM_TASKS};

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
                let (tx, rx) = oneshot::channel::<JsonB>();
                channel.send(Message::ReturnHandler(
                    setup_handler.clone(),
                    PostgresSender::Json(tx),
                    Arc::new(job.clone()),
                ));

                match rx.await {
                    Ok(json) => match serde_json::from_value(json.0) {
                        // Update options
                        Ok(options) => job.options = options,
                        Err(error) => {
                            warning!("sync: svm: tasks: {}: failed to parse return handler options jsonb with {}", &job.name, &error);
                            continue;
                        }
                    },
                    Err(error) => {
                        warning!("sync: svm: tasks: {}: setup handler failed with {}", &job.name, error);
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
                    handle_blocks_task(job.clone(), &channel).await
                } else if job.options.is_log_job() {
                    Err(anyhow!("log tasks are not supported yet"))
                } else if job.options.is_transaction_job() {
                    handle_transactions_task(job.clone(), &channel).await
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
                            success_handler.clone(),
                            tx,
                            job.clone(),
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
                            failure_handler.clone(),
                            tx,
                            job.clone(),
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
    let Some(options) = &job.options.svm else {
        bail!("sync: svm: tasks: {}: job options are not set", &job.name);
    };

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
                    channel.send(Message::SvmBlock(block, job.clone()));
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
    let Some(options) = &job.options.svm else {
        bail!("sync: svm: tasks: {}: job options are not set", &job.name);
    };

    let rpc = Arc::new(job.reconnect_svm_rpc().await);

    let Some(mentions) = &options.mentions else {
        bail!("mentions field is required for transaction task");
    };

    let mut signatures: IndexSet<Signature> = IndexSet::new();

    log!(
        "sync: svm: tasks: {}: getting signatures for {} mentions",
        &job.name,
        mentions.len()
    );

    for (i, mention) in mentions.iter().enumerate() {
        let address = Pubkey::from_str(mention)?;
        let mut before: Option<Signature> = None;
        let mut until: Option<Signature> = None;

        if let Some(before_signatures) = &options.before {
            if let Some(signature) = before_signatures.get(i) {
                before = Some(Signature::from_str(signature)?);
            }
        }

        if let Some(until_signatures) = &options.until {
            if let Some(signature) = until_signatures.get(i) {
                until = Some(Signature::from_str(signature)?);
            }
        }

        let mut retries = 0;
        'fill: loop {
            ensure!(
                retries < 10,
                "failed to get signatures after 10 retries, aborting..."
            );

            log!(
                "sync: svm: tasks: {}: getting signatures for {}, {} / {}",
                &job.name,
                mention,
                i + 1,
                mentions.len()
            );

            match rpc.get_signatures_for_address_with_config(
              &address,
              solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                  before,
                  until,
                  limit: Some(1000),
                  commitment: Some(solana_sdk::commitment_config::CommitmentConfig {
                      commitment: CommitmentLevel::Finalized,
                  }),
              }).await {
                Ok(sigs) => {
                  if sigs.is_empty() {
                    break 'fill;
                  }

                  log!(
                    "sync: svm: tasks: {}: {}: got {} signatures",
                    &job.name,
                    mention,
                    sigs.len()
                  );

                  before = Signature::from_str(sigs[sigs.len() - 1].signature.as_str())?.into();

                  // Insert signatures from newest to oldest (will be reversed later)
                  for signature in sigs.iter() {
                    let signature = Signature::from_str(signature.signature.as_str())
                        .expect("signature to be parsed");
                    signatures.insert(signature);
                  }

                  retries = 0;
                },
                Err(error) => {
                  warning!("sync: svm: tasks: {}: failed to get signatures with {}", &job.name, error);

                  sleep_until(
                      Instant::now() + Duration::from_millis(1000),
                  )
                  .await;
                  retries += 1;
                  continue;
                }
            }
        }
    }

    let mut queue: Vec<
        oneshot::Receiver<EncodedConfirmedTransactionWithStatusMeta>,
    > = Vec::new();

    let mut skipped_signatures = 0;
    if let Some(handler) = &options.transaction_skip_lookup {
        for signature in std::mem::take(&mut signatures) {
            let (tx, rx) = oneshot::channel::<String>();
            ensure!(
                channel.send(Message::ReturnHandlerWithArg(
                    PostgresArg::String(signature.to_string()),
                    handler.clone(),
                    PostgresSender::String(tx),
                    job.clone()
                )),
                "sync: svm: tasks: {}: failed to send return handler with arg",
                &job.name
            );

            if let Err(_) = rx.await {
                signatures.insert(signature);
            } else {
                skipped_signatures += 1;
            }
        }
    } else {
        queue.reserve_exact(signatures.len());
    }

    // Reverse the signatures to fetch them in oldest to newest order
    signatures.reverse();

    ensure!(
        !signatures.is_empty(),
        "sync: svm: tasks: {}: no signatures found",
        &job.name
    );

    log!(
        "sync: svm: tasks: {}: got {} ({} skipped) signatures total",
        &job.name,
        signatures.len(),
        skipped_signatures
    );

    let config = transactions::build_config(&job.options);
    let semaphore = Arc::new(Semaphore::new(SVM_RPC_PERMITS.get() as usize));

    // Reverse the signatures to fetch them in oldest to newest order
    for signature in signatures.drain(..) {
        let (tx, rx) = oneshot::channel();
        queue.push(rx);

        let semaphore = Arc::clone(&semaphore);
        let rpc = Arc::clone(&rpc);
        let job = Arc::clone(&job);
        tokio::spawn(async move {
            let permit = semaphore.acquire().await;
            loop {
                match rpc.get_transaction_with_config(&signature, config).await
                {
                    Ok(transaction) => {
                        tx.send(transaction).expect("sender got dropped");
                    }
                    Err(error) => {
                        warning!(
                            "sync: svm: tasks: {}: {}: failed to get transaction with {}, retrying",
                            &job.name,
                            &signature,
                            error
                        );

                        sleep_until(
                            Instant::now() + Duration::from_millis(1000),
                        )
                        .await;
                        continue;
                    }
                };

                break;
            }

            drop(permit);
        });
    }

    drop(signatures);

    let count = queue.len();
    for (index, rx) in queue.iter_mut().enumerate() {
        if index % 100 == 0 {
            log!(
                "sync: svm: tasks: {}: progress {} / {}",
                &job.name,
                index,
                count
            );
        }

        let Ok(tx) = rx.await else {
            bail!("failed to receive transaction");
        };

        match tx.try_into() as Result<SvmTransaction, anyhow::Error> {
            Ok(tx) => {
                if !tx.failed {
                    channel.send(Message::SvmTransaction(tx, job.clone()));
                }
            }
            Err(error) => {
                bail!(
                  "sync: svm: tasks: {}: transaction validation failed with {}",
                  &job.name,
                  error
                );
            }
        };
    }

    Ok(())
}
