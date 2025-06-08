use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic::Ordering, Arc};

use anyhow::{anyhow, bail, ensure};

use pgrx::{log, warning, JsonB};

use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig,
};
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, Duration, Instant};

use super::transactions::stream_transactions;
use super::{blocks, SvmTransaction};

use crate::channel::{unbounded_ordered_channel, Channel};
use crate::svm::SvmAccount;
use crate::types::*;
use crate::worker::{SVM_SIGNATURES_BUFFER, SVM_TASKS};

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
                } else if job.options.is_accounts_job() {
                    handle_accounts_task(job.clone(), &channel).await
                } else if job.options.is_transaction_job() {
                    handle_transactions_task(job.clone(), &channel).await
                } else {
                    Err(anyhow!("unknown task"))
                }
            };

            match task {
                Ok(()) => {
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

                    log!("sync: svm: tasks: {}: task completed", &job.name);
                }
                Err(error) => {
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

                    warning!(
                        "sync: svm: tasks: {}: task failed with {}",
                        &job.name,
                        error
                    );
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
    if job.options.svm.is_none() {
        bail!("sync: svm: tasks: {}: job options are not set", &job.name);
    };

    let rpc = Arc::new(job.reconnect_svm_rpc().await);

    let (send_signature, mut receive_signature) =
        mpsc::channel::<Signature>(SVM_SIGNATURES_BUFFER.get() as usize);
    let (tx, mut rx) = unbounded_ordered_channel::<SvmTransaction>();

    let transaction_count = Arc::new(AtomicUsize::new(0));
    let signatures_job = job.clone();
    let signatures_count = transaction_count.clone();
    let (signatures, transactions, inserts) = tokio::join!(
        async move {
            let _ = send_signature;

            let Some(options) = &signatures_job.options.svm else {
                bail!(
                    "sync: svm: tasks: {}: job options are not set",
                    &signatures_job.name
                );
            };

            let Some(mentions) = &options.mentions else {
                bail!(
                    "sync: svm: tasks: {}: mentions are not set",
                    &signatures_job.name
                );
            };

            log!(
                "sync: svm: tasks: {}: getting signatures for {} mentions",
                &signatures_job.name,
                mentions.len(),
            );

            for (i, mention) in mentions.iter().enumerate() {
                let address = Pubkey::from_str(mention)?;
                let mut before = {
                    if let Some(before_signatures) = &options.before {
                        if let Some(signature) = before_signatures.get(i) {
                            Some(Signature::from_str(signature)?)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };

                let until = {
                    if let Some(until_signatures) = &options.until {
                        if let Some(signature) = until_signatures.get(i) {
                            Some(Signature::from_str(signature)?)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };

                let mut retries = 0;
                let mut batch = 0;
                loop {
                    ensure!(
                        retries < 10,
                        "failed to get signatures after 10 retries, aborting..."
                    );

                    log!(
                        "sync: svm: tasks: {}: getting signatures for {} [{}]",
                        &signatures_job.name,
                        address,
                        batch,
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
                            Ok(confirmed) => {
                              if confirmed.is_empty() {
                                break;
                              }

                              log!(
                                "sync: svm: tasks: {}: {}: got {} signatures",
                                &signatures_job.name,
                                address,
                                confirmed.len()
                              );

                              before = Signature::from_str(confirmed[confirmed.len() - 1].signature.as_str())?.into();
                              for status in confirmed {
                                  let signature = Signature::from_str(status.signature.as_str())?;

                                  // Sometimes we can skip the lookup so we don't have to re-handle the same signature
                                  if let Some(handler) = &options.transaction_skip_lookup {
                                      let (tx, rx) = oneshot::channel::<String>();
                                      ensure!(
                                          channel.send(Message::ReturnHandlerWithArg(
                                              PostgresArg::String(status.signature),
                                              handler.clone(),
                                              PostgresSender::String(tx),
                                              signatures_job.clone()
                                          )),
                                          "failed to lookup transaction through skip handler",
                                      );

                                      if rx.await.is_ok() {
                                          continue; // Skip this signature
                                      }
                                  }

                                  send_signature.send(signature).await?;
                                  signatures_count.fetch_add(1, Ordering::Relaxed);
                              }

                              retries = 0;
                              batch += 1;
                            },
                            Err(error) => {
                              warning!("sync: svm: tasks: {}: failed to get signatures with {}", &signatures_job.name, error);

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

            Ok(())
        },
        stream_transactions(&mut receive_signature, tx, job.clone()),
        async move {
            let mut current = 0;
            while let Some(transaction) = rx.recv().await {
                let count = transaction_count.load(Ordering::Relaxed);
                if current % 100 == 0 {
                    log!(
                        "sync: svm: tasks: {}: {} / {} signatures processed",
                        job.name,
                        current,
                        count
                    );
                }

                if !transaction.failed {
                    ensure!(
                        channel.send(Message::SvmTransaction(
                            transaction,
                            job.clone()
                        )),
                        "failed to send insert transaction message"
                    );
                }

                current += 1;
            }

            Ok(())
        },
    );

    signatures?;
    transactions?;
    inserts?;

    Ok(())
}

async fn handle_accounts_task(
    job: Arc<Job>,
    channel: &Arc<Channel>,
) -> Result<(), anyhow::Error> {
    let Some(options) = &job.options.svm else {
        bail!("sync: svm: tasks: {}: job options are not set", &job.name);
    };

    let rpc = job.reconnect_svm_rpc().await;

    let Some(program) = options.program else {
        bail!("program is required for accounts task");
    };

    let Some(filters) = &options.accounts_filters else {
        bail!("filters are required for accounts task");
    };

    match rpc
        .get_program_accounts_with_config(
            &program,
            solana_client::rpc_config::RpcProgramAccountsConfig {
                filters: Some(filters.clone()),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64Zstd),
                    data_slice: options.accounts_data_slice,
                    ..RpcAccountInfoConfig::default()
                },
                ..RpcProgramAccountsConfig::default()
            },
        )
        .await
    {
        Ok(accounts) => {
            log!(
                "sync: svm: tasks: {}: got {} accounts for program {}",
                &job.name,
                accounts.len(),
                program
            );

            for (pubkey, account) in accounts {
                let account = SvmAccount {
                    address: pubkey,
                    inner: account,
                };

                ensure!(
                    channel.send(Message::SvmAccount(account, job.clone())),
                    "failed to send account message"
                );
            }
        }
        Err(error) => {
            warning!(
                "sync: svm: tasks: {}: failed to get accounts with {}",
                &job.name,
                error
            );
        }
    }

    Ok(())
}
