use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::prelude::*;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_stream::StreamExt;

use chrono::Utc;
use cron::Schedule;

use bus::Bus;

use crate::anyhow_pg_try;
use crate::channel::*;
use crate::evm;
use crate::query::{PgHandler, PgResult};
use crate::svm;
use crate::types::*;
use crate::worker;
use crate::worker::*;

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn background_worker_sync(_arg: pg_sys::Datum) {
    // Auto-quit after n restarts, require manual restart
    if *RESTART_COUNT.exclusive() >= 5 {
        return;
    }

    *RESTART_COUNT.exclusive() += 1;

    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );

    if let Some(database) = DATABASE.get() {
        BackgroundWorker::connect_worker_to_spi(
            Some(database.to_str().expect("database name to be valid utf8")),
            None,
        );
    } else {
        error!("sync: database name was not provided");
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("sync: Failed to create async runtime");

    log!("sync: worker has started!");

    let jobs = match anyhow_pg_try!(|| Job::query_all()) {
        Ok(jobs) => jobs,
        Err(error) => error!("sync: failed to query jobs with {}", error),
    };

    for job in &jobs {
        let id = job.id.clone();
        if let Err(error) =
            anyhow_pg_try!(|| Job::update(id, JobStatus::Stopped))
        {
            warning!("sync: {}: failed to stop with {}", job.id, error);
        }
    }

    log!("sync: {} jobs found", jobs.len());

    *WORKER_STATUS.exclusive() = WorkerStatus::RUNNING;

    let (send_message, receive_message) =
        mpsc::channel::<Message>(MESSAGES_CAPACITY);

    let mut signal_bus = Bus::<Signal>::new(64);

    let channel = Arc::new(Channel::new(send_message));

    runtime.block_on(async {
        let evm_blocks_rx = signal_bus.add_rx();
        let evm_logs_rx = signal_bus.add_rx();

        let svm_blocks_rx = signal_bus.add_rx();
        let svm_logs_rx = signal_bus.add_rx();

        let scheduler = tokio::spawn(schedule_tasks(Arc::clone(&channel)));
        let handler =
            tokio::spawn(handle_message(MessageStream::new(receive_message)));

        preload_tasks(jobs.tasks());
        tokio::select! {
             _ = worker::handle_signals(Arc::clone(&channel), signal_bus) => {
                 log!("sync: received exit signal... exiting");
             },
             _ = evm::blocks::listen(Arc::clone(&channel), evm_blocks_rx) => {
                 log!("sync: stopped listening to blocks... exiting");
             },
             _ = evm::logs::listen(Arc::clone(&channel), evm_logs_rx) => {
                 log!("sync: stopped listening to events... exiting");
             },
             _ = svm::blocks::listen(Arc::clone(&channel), svm_blocks_rx) => {
                 log!("sync: stopped listening to blocks... exiting");
             },
             _ = svm::logs::listen(Arc::clone(&channel), svm_logs_rx) => {
                 log!("sync: stopped listening to transactions... exiting");
             },
             _ = evm::tasks::handle_tasks(Arc::clone(&channel)) => {
                 log!("sync: tasks: stopped tasks... exiting");
             },
             _ = svm::tasks::handle_tasks(Arc::clone(&channel)) => {
                 log!("sync: tasks: stopped tasks... exiting");
             },
        }

        scheduler.abort();
        if channel.send(Message::Shutdown) {
            if let Err(err) = handler.await {
                log!("sync: messages: exited with error: {}", err);
            }
        }
    });

    for job in &jobs {
        let id = job.id.clone();
        if let Err(error) =
            anyhow_pg_try!(|| Job::update(id, JobStatus::Stopped))
        {
            warning!("sync: {}: failed to stop with {}", job.id, error);
        }
    }

    *WORKER_STATUS.exclusive() = WorkerStatus::STOPPED;
    log!("sync: worker has exited");
}

pub fn preload_tasks(tasks: Vec<Job>) {
    for task in tasks {
        if matches!(task.options.evm, Some(true)) {
            EVM_TASKS.exclusive().push(task.id).unwrap();
        } else if matches!(task.options.svm, Some(true)) {
            SVM_TASKS.exclusive().push(task.id).unwrap();
        }
    }
}

pub async fn schedule_tasks(channel: Arc<Channel>) {
    let mut handles: HashMap<i64, JoinHandle<()>> = HashMap::new();

    loop {
        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        channel.send(Message::Jobs(tx));

        let Ok(jobs) = rx.await else {
            warning!("sync: tasks: failed to get jobs, retrying...");
            continue;
        };

        let tasks = jobs.tasks();
        handles.retain(|id, handle| {
            match tasks.iter().find(|job| &job.id == id) {
                Some(_) => true,
                None => {
                    handle.abort();
                    log!("sync: tasks: {}: task was removed", id);
                    false
                }
            }
        });

        for task in tasks {
            if let Some(cron) = &task.options.cron {
                let Ok(schedule) = Schedule::from_str(&cron) else {
                    warning!(
                        "sync: tasks: {}: has incorrect cron expression {}",
                        task.name,
                        &cron
                    );

                    continue;
                };

                // Register cron task
                handles.entry(task.id).or_insert_with(|| {
                    log!(
                        "sync: tasks: {}: scheduled with {}",
                        &task.name,
                        cron
                    );

                    tokio::spawn(async move {
                        loop {
                            let upcoming =
                                schedule.upcoming(Utc).next().unwrap();
                            let duration = upcoming - Utc::now();

                            tokio::time::sleep(duration.to_std().unwrap())
                                .await;

                            if matches!(task.options.evm, Some(true)) {
                                EVM_TASKS.exclusive().push(task.id).unwrap();
                            } else if matches!(task.options.svm, Some(true)) {
                                SVM_TASKS.exclusive().push(task.id).unwrap();
                            }
                        }
                    })
                });
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn handle_message(mut stream: MessageStream) {
    let evm_blocks = Arc::new(AtomicUsize::new(0));
    let evm_logs = Arc::new(AtomicUsize::new(0));
    let evm_blocks_stats = Arc::clone(&evm_blocks);
    let evm_logs_stats = Arc::clone(&evm_logs);

    let svm_blocks = Arc::new(AtomicUsize::new(0));
    let svm_logs = Arc::new(AtomicUsize::new(0));
    let svm_txs = Arc::new(AtomicUsize::new(0));
    let svm_blocks_stats = Arc::clone(&svm_blocks);
    let svm_logs_stats = Arc::clone(&svm_logs);
    let svm_txs_stats = Arc::clone(&svm_txs);

    // Spawn stats task
    let stats = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;
            log!(
                "sync: messages: evm: blocks={}/m logs={}/m svm: blocks={}/m txs={}/m logs={}/m",
                evm_blocks_stats.swap(0, Ordering::Relaxed),
                evm_logs_stats.swap(0, Ordering::Relaxed),
                svm_blocks_stats.swap(0, Ordering::Relaxed),
                svm_txs_stats.swap(0, Ordering::Relaxed),
                svm_logs_stats.swap(0, Ordering::Relaxed)
            );
        }
    });

    loop {
        let Some(message) = stream.next().await else {
            warning!("sync: messages: stream is dead");
            break;
        };

        match message {
            Message::Job(id, oneshot) => {
                match anyhow_pg_try!(|| Job::query_all()) {
                    Ok(jobs) => {
                        if oneshot
                            .send(jobs.into_iter().find(|job| job.id == id))
                            .is_err()
                        {
                            warning!("sync: messages: failed to return a job");
                        }
                    }
                    Err(error) => {
                        warning!(
                            "sync: messages: failed to retrieve jobs with {}",
                            error
                        );
                    }
                }
            }

            Message::Jobs(oneshot) => match anyhow_pg_try!(|| Job::query_all())
            {
                Ok(jobs) => {
                    if oneshot.send(jobs).is_err() {
                        warning!("sync: messages: failed to return jobs");
                    }
                }
                Err(error) => {
                    warning!(
                        "sync: messages: failed to retrieve jobs with {}",
                        error
                    );
                }
            },
            Message::UpdateJob(job, status) => {
                if let Err(error) = anyhow_pg_try!(|| Job::update(job, status))
                {
                    warning!(
                        "sync: messages: {}: updating status failed with {}",
                        job,
                        error
                    );
                }
            }
            Message::EvmBlock(block, job) => {
                evm_blocks.fetch_add(1, Ordering::Relaxed);
                log!(
                    "sync: evm: blocks: {}: adding {}",
                    &job.name,
                    &block.number
                );

                let Some(handler) = job.options.block_handler.as_ref() else {
                    error!("sync: evm: blocks: {}: missing handler", job.name);
                };

                let id = job.id;
                if let Err(error) =
                    anyhow_pg_try!(|| block.call_handler(&handler, id))
                {
                    warning!(
                        "sync: evm: blocks: {}: block handler failed with {}",
                        job.id,
                        error
                    );
                }
            }
            Message::EvmLog(log, job) => {
                evm_logs.fetch_add(1, Ordering::Relaxed);
                log!(
                    "sync: evm: logs: {}: adding {}<{}>",
                    &job.name,
                    log.transaction_hash.as_ref().unwrap(),
                    log.log_index.as_ref().unwrap()
                );

                let Some(handler) = job.options.log_handler.as_ref() else {
                    error!("sync: evm: logs: {}: missing handler", job.name);
                };

                let id = job.id;
                if let Err(error) =
                    anyhow_pg_try!(|| log.call_handler(&handler, id))
                {
                    warning!(
                        "sync: evm: logs: {}: log handler failed with {}",
                        job.id,
                        error
                    );
                }
            }
            Message::SvmBlock(block, job) => {
                svm_blocks.fetch_add(1, Ordering::Relaxed);
                log!(
                    "sync: svm: blocks: {}: adding {}",
                    job.name,
                    block.block_height.as_ref().unwrap()
                );

                let Some(handler) = job.options.block_handler.as_ref() else {
                    error!("sync: svm: blocks: {}: missing handler", job.name);
                };

                let id = job.id;
                if let Err(error) =
                    anyhow_pg_try!(|| block.call_handler(&handler, id))
                {
                    warning!(
                        "sync: evm: blocks: {}: block handler failed with {}",
                        job.id,
                        error
                    );
                }
            }
            Message::SvmLog(log, job) => {
                svm_logs.fetch_add(1, Ordering::Relaxed);
                svm_blocks.fetch_add(1, Ordering::Relaxed);
                log!(
                    "sync: svm: logs: {}: adding {}",
                    job.name,
                    log.context.slot
                );

                let Some(handler) = job.options.log_handler.as_ref() else {
                    error!("sync: svm: blocks: {}: missing handler", job.name);
                };

                let id = job.id;
                if let Err(error) =
                    anyhow_pg_try!(|| log.call_handler(&handler, id))
                {
                    warning!(
                        "sync: svm: logs: {}: log handler failed with {}",
                        job.id,
                        error
                    );
                }
            }
            Message::SvmTransaction(message, job) => {
                svm_txs.fetch_add(1, Ordering::Relaxed);
                svm::transactions::handle_transaction_message(message, job);
            }
            Message::Handler(job_id, handler, oneshot) => {
                let success =
                    match anyhow_pg_try!(|| Job::handler(&handler, job_id)) {
                        Ok(_) => true,
                        Err(error) => {
                            warning!(
                                "sync: messages: {}: {} failed with {}",
                                job_id,
                                handler,
                                error
                            );
                            false
                        }
                    };

                if let Err(_) = oneshot.send(success) {
                    warning!(
                        "sync: messages: {}: {} failed to return, task is probably deadlocked!",
                        job_id, handler
                    );
                }
            }
            Message::JsonHandler(job_id, handler, oneshot) => {
                let result = match anyhow_pg_try!(|| Job::json_handler(
                    &handler, job_id
                )) {
                    Ok(result) => Some(result),
                    Err(error) => {
                        warning!(
                            "sync: messages: {}: {} failed with {}",
                            job_id,
                            handler,
                            error
                        );
                        None
                    }
                };

                if let Err(_) = oneshot.send(result) {
                    warning!(
                        "sync: messages: {}: {} failed to return, task is probably deadlocked!",
                        job_id, handler
                    );
                }
            }
            Message::CheckBlock(number, oneshot, job) => {
                if let Some(handler) = job.options.block_check_handler.as_ref()
                {
                    let id = job.id;

                    let found = match anyhow_pg_try!(|| {
                        u64::call_handler(&number, handler, id)
                    }) {
                        Ok(found) => matches!(found, PgResult::Boolean(true)),
                        Err(_) => false,
                    };

                    if let Err(_) = oneshot.send(found) {
                        warning!(
                            "sync: messages: {}: check block failed to return with, task is probably deadlocked!",
                            &job.name
                        );
                    }
                }
            }
            Message::Shutdown => {
                break;
            }
        }
    }

    stats.abort();
}
