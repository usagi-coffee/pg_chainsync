use std::str::FromStr;
use std::sync::Arc;

use alloy::providers::Provider;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{log, warning};

use tokio::sync::oneshot;
use tokio::time::{sleep_until, Duration, Instant};

use cron::Schedule;
use tokio_cron::{Job as CronJob, Scheduler};

use crate::channel::Channel;
use crate::sync::evm::logs;
use crate::types::*;

use crate::worker::EVM_TASKS;

pub async fn setup(scheduler: &mut Scheduler) {
    let tasks = BackgroundWorker::transaction(|| Job::query_all());
    if tasks.is_err() {
        warning!("sync: evm: tasks: failed to setup tasks");
        return;
    }

    // turn tasks to arc
    // SAFETY: we checked error before, avoiding indent..
    let tasks = tasks
        .unwrap()
        .evm_jobs()
        .tasks()
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

    for task in tasks {
        let id = task.id;

        // Enqueue preloaded tasks
        if matches!(task.options.preload, Some(true)) {
            if EVM_TASKS.exclusive().push(task.id).is_err() {
                warning!("sync: evm: tasks: failed to enqueue {}", task.id);
            }
        }

        // Cron
        if let Some(cron) = &task.options.cron {
            log!("sync: evm: tasks: {} ", id);
            if Schedule::from_str(&cron).is_err() {
                warning!(
                    "sync: evm: tasks: task {} has incorrect cron expression {}",
                    task.id,
                    cron
                );

                continue;
            }

            log!("sync: evm: tasks: {}: scheduling {}", id, cron);
            scheduler.add(CronJob::new_sync(cron, move || {
                if EVM_TASKS.exclusive().push(id).is_err() {
                    warning!("sync: evm: tasks: failed to enqueue {}", id);
                }
            }));
        }
    }
}

pub async fn handle_tasks(channel: Arc<Channel>) {
    loop {
        let task: Option<i64> = { EVM_TASKS.exclusive().pop() };

        if let Some(task) = task {
            log!("sync: evm: tasks: got task {}", task);

            // FIXME: wait some time for commit when adding tasks
            sleep_until(Instant::now() + Duration::from_millis(100)).await;

            let (tx, rx) = oneshot::channel::<Option<Job>>();
            channel.send(Message::Job(task, tx));

            let job = rx.await;

            if let Err(_) = job {
                warning!(
                    "sync: evm: tasks: failed to fetch job for task {}",
                    task
                );
                continue;
            }

            let job = job.unwrap();

            if job.is_none() {
                warning!(
                    "sync: evm: tasks: failed to find job for task {}",
                    task
                );
                continue;
            }

            let job = job.unwrap();
            if let Err(err) = job.connect_evm().await {
                warning!(
                    "sync: evm: tasks: failed to create provider for {}, {}",
                    task,
                    err
                );
                continue;
            };

            let job = Arc::new(job);

            if job.options.is_block_job() {
                handle_blocks_task(Arc::clone(&job), &channel).await
            } else if job.options.is_log_job() {
                handle_log_task(Arc::clone(&job), &channel).await
            } else {
                warning!("sync: evm: tasks: unknown  task {}", task);
            }

            channel.send(Message::TaskSuccess(Arc::clone(&job)));
        }

        sleep_until(Instant::now() + Duration::from_millis(250)).await;
    }
}

async fn handle_blocks_task(job: Arc<Job>, channel: &Arc<Channel>) {
    let options = &job.options;

    let mut to = options.to_block.unwrap_or(0);
    if options.to_block.is_none() {
        to = job
            .connect_evm()
            .await
            .unwrap()
            .get_block_number()
            .await
            .unwrap() as i64;
    }

    for i in options.from_block.unwrap()..to {
        if let Ok(block) = job
            .connect_evm()
            .await
            .unwrap()
            .get_block(
                (i as u64).into(),
                alloy::rpc::types::BlockTransactionsKind::Hashes,
            )
            .await
        {
            if let Some(block) = block {
                channel.send(Message::EvmBlock(block.header, Arc::clone(&job)));
            }
        }
    }
}

async fn handle_log_task(job: Arc<Job>, channel: &Arc<Channel>) {
    let options = &job.options;

    // SAFETY: before we connected so we are safe to do all these crazy things
    let block = job
        .connect_evm()
        .await
        .unwrap()
        .get_block_number()
        .await
        .unwrap() as u64;

    let mut filter = logs::build_filter(options, block);

    let from_block = options.from_block.unwrap_or(0);
    let mut to_block = options.to_block.unwrap_or(0);
    if options.to_block.is_none() {
        to_block = block as i64;
    }

    // Split logs by blocktick if needed
    if let Some(blocktick) = options.blocktick {
        let recalculate_splits = |from: i64, to: i64, blocktick: i64| {
            ((to - from) as f64 / blocktick as f64).ceil() as i64
        };

        let mut current_blocktick = blocktick;
        let mut current_from = from_block;
        let mut splits = recalculate_splits(from_block, to_block, blocktick);

        log!("sync: evm: tasks: {}: found {} splits", job.id, splits);

        let mut retries = 0;
        let mut i = 1;
        while i <= splits {
            let mut from = current_from + (i - 1) * current_blocktick;
            let to = std::cmp::min(to_block, from + current_blocktick);

            if i > 1 {
                from = from + 1;
            }

            log!(
                "sync: evm: tasks: {}: fetching blocks {} to {} ({} / {})",
                job.id,
                from,
                to,
                i,
                splits
            );

            filter = filter.from_block(from as u64).to_block(to as u64);

            let logs = job.connect_evm().await.unwrap().get_logs(&filter).await;

            match logs {
                Ok(mut logs) => {
                    retries = 0;
                    for log in logs.drain(0..) {
                        logs::handle_evm_log(&job, log, &channel).await;
                    }
                }
                Err(e) => {
                    println!("{}", e);
                    if current_blocktick <= 1 || retries >= 20 {
                        warning!(
                          "sync: evm: tasks: {}: failed to fetch with reduced blocktick, aborting...",
                          job.id,
                      );

                        break;
                    }

                    log!(
                        "sync: evm: tasks: {}: reducing blocktick from {} to {}",
                        job.id,
                        current_blocktick,
                        (current_blocktick as f64 / 2 as f64).floor(),
                    );

                    sleep_until(Instant::now() + Duration::from_millis(200))
                        .await;

                    current_blocktick =
                        (current_blocktick as f64 / 2 as f64).floor() as i64;
                    current_from = from;
                    splits = recalculate_splits(
                        current_from,
                        to_block,
                        current_blocktick,
                    );
                    retries = 0;
                    i = 1;
                    continue;
                }
            }

            i = i + 1;
        }
    }
    // Or just get all logs at once
    else {
        match job.connect_evm().await.unwrap().get_logs(&filter).await {
            Ok(mut logs) => {
                for log in logs.drain(0..) {
                    logs::handle_evm_log(&job, log, &channel).await;
                }
            }
            Err(e) => {
                log!("{}", e);
                warning!(
                    "sync: evm: tasks: failed to get logs for {}, aborting...",
                    job.id
                );
                channel.send(Message::TaskFailure(Arc::clone(&job)));
            }
        }
    }
}
