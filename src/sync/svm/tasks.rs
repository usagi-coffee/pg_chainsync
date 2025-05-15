use std::str::FromStr;
use std::sync::Arc;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{log, warning, PgTryBuilder};

use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

use tokio::sync::oneshot;
use tokio::time::{sleep_until, Duration, Instant};
use tokio_cron::{Job as CronJob, Scheduler};

use chrono::Utc;
use cron::Schedule;

use super::blocks::build_config;
use crate::anyhow_pg_try;
use crate::channel::Channel;
use crate::types::*;
use crate::worker::SVM_TASKS;

pub async fn setup(scheduler: &mut Scheduler) {
    let Ok(jobs) = anyhow_pg_try!(|| Job::query_all()) else {
        warning!("sync: svm: tasks: failed to setup tasks");
        return;
    };

    let tasks = jobs.svm_jobs().tasks().into_iter();
    for task in tasks {
        let id = task.id;
        // Enqueue preloaded tasks
        if matches!(task.options.preload, Some(true)) {
            if let Ok(_) = SVM_TASKS.exclusive().push(task.id) {
                log!("sync: svm: svm: tasks: enqueued {}", task.id);
            } else {
                warning!("sync: svm: tasks: failed to enqueue {}", task.id);
            }
        }

        // Cron
        if let Some(cron) = &task.options.cron {
            let Ok(schedule) = Schedule::from_str(&cron) else {
                warning!(
                    "sync: svm: tasks: task {} has incorrect cron expression {}",
                    task.id,
                    cron
                );

                continue;
            };

            log!(
                "sync: tasks: {}: scheduling {}, next at {}",
                id,
                cron,
                schedule.upcoming(Utc).next().unwrap()
            );

            scheduler.add(CronJob::new_sync(cron, move || {
                if SVM_TASKS.exclusive().push(id).is_err() {
                    warning!("sync: svm: tasks: failed to enqueue {}", id);
                }
            }));
        }
    }
}

pub async fn handle_tasks(channel: Arc<Channel>) {
    loop {
        let task: Option<i64> = { SVM_TASKS.exclusive().pop() };

        if let Some(task) = task {
            log!("sync: svm: tasks: got task {}", task);

            // FIXME: wait some time for commit when adding tasks
            sleep_until(Instant::now() + Duration::from_millis(100)).await;

            let (tx, rx) = oneshot::channel::<Option<Job>>();
            channel.send(Message::Job(task, tx));

            let Ok(job) = rx.await else {
                warning!(
                    "sync: svm: tasks: failed to fetch job for task {}",
                    task
                );
                continue;
            };

            let Some(job) = job else {
                warning!(
                    "sync: svm: tasks: failed to find job for task {}",
                    task
                );
                continue;
            };

            if let Err(err) = job.connect_svm_rpc().await {
                warning!(
                    "sync: svm: tasks: failed to create provider for {}, {}",
                    task,
                    err
                );
                continue;
            };

            let job = Arc::new(job);

            if job.options.is_block_job() {
                handle_blocks_task(Arc::clone(&job), &channel).await;
            } else if job.options.is_log_job() {
                warning!("sync: svm: tasks: unknown  task {}", task);
            } else if job.options.is_transaction_job() {
                handle_transactions_task(Arc::clone(&job), &channel).await;
            } else {
                warning!("sync: svm: tasks: unknown  task {}", task);
            }

            channel.send(Message::TaskSuccess(Arc::clone(&job)));
        }

        sleep_until(Instant::now() + Duration::from_millis(250)).await;
    }
}

async fn handle_blocks_task(job: Arc<Job>, channel: &Arc<Channel>) {
    let options = &job.options;

    let from_slot = options
        .from_slot
        .expect("sync: svm: tasks: from_slot is required for task");

    let mut to_slot = options.to_slot.unwrap_or(0);
    if options.to_slot.is_none() {
        to_slot = job
            .connect_svm_rpc()
            .await
            .unwrap()
            .get_slot()
            .await
            .expect("Failed to get block height");
    }

    let rpc = job.connect_svm_rpc().await.unwrap();
    for i in from_slot..to_slot {
        match rpc
            .get_block_with_config(i as u64, build_config(&options))
            .await
        {
            Ok(block) => {
                channel.send(Message::SvmBlock(block, Arc::clone(&job)));
            }
            Err(err) => {
                warning!(
                    "sync: svm: tasks: blocks: failed to get {}, {}",
                    i,
                    err
                );
            }
        }
    }
}

async fn handle_transactions_task(job: Arc<Job>, channel: &Arc<Channel>) {
    let rpc = job.connect_svm_rpc().await.unwrap();

    let address =
        Pubkey::from_str(job.options.mentions.as_ref().unwrap()[0].as_str())
            .expect("Failed to parse signature");

    let mut before: Option<Signature> = None;
    let mut until: Option<Signature> = None;

    if let Some(before_signature) = job.options.before.as_ref() {
        before = Some(
            Signature::from_str(before_signature)
                .expect("Failed to parse signature"),
        );
    }

    if let Some(until_signature) = job.options.until.as_ref() {
        until = Some(
            Signature::from_str(until_signature)
                .expect("Failed to parse signature"),
        );
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

          before = Signature::from_str(sigs[sigs.len() - 1].signature.as_str()).expect("Failed to parse signature").into();

          signatures.extend(sigs);
        },
        Err(_) => {
          warning!("sync: svm: tasks: failed to get signatures");
          break 'fill;
        }
      }
    }

    for signature in signatures {
        let signature = Signature::from_str(signature.signature.as_str())
            .expect("Failed to parse signature");

        let tx = job
            .connect_svm_rpc()
            .await
            .expect("Failed retrieve rpc client")
            .get_transaction_with_config(
                &signature,
                crate::sync::svm::transactions::build_config(&job.options),
            )
            .await;

        match tx {
            Ok(tx) => {
                let meta =
                    tx.transaction.meta.as_ref().expect("Failed to get meta");

                if meta.err.is_none() {
                    channel.send(Message::SvmTransaction(tx, Arc::clone(&job)));
                }
            }
            Err(err) => {
                warning!(
                    "sync: svm: logs: {}: failed to get transaction {}",
                    &job.name,
                    err
                );
            }
        }
    }
}
