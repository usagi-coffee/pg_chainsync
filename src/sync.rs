use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pgrx::prelude::*;

use pgrx::bgworkers::*;
use pgrx::log;

use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_cron::Scheduler;
use tokio_stream::StreamExt;

use bus::Bus;

use crate::channel::*;
use crate::query::{PgHandler, PgResult};
use crate::types::*;
use crate::worker;
use crate::worker::*;

pub mod evm;
pub mod svm;

const DATABASE: &str = "postgres";

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

    BackgroundWorker::connect_worker_to_spi(Some(DATABASE), None);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("sync: Failed to create async runtime");

    log!("sync: worker has started!");

    let jobs = BackgroundWorker::transaction(|| {
        PgTryBuilder::new(Job::query_all)
            .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
            .execute()
    })
    .unwrap_or(Vec::new());

    for job in &jobs {
        let id = job.id.clone();
        BackgroundWorker::transaction(|| {
            let _ = PgTryBuilder::new(|| Job::update(id, JobStatus::Stopped))
                .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                .execute();
        });
    }

    log!("sync: {} jobs found", jobs.len());
    log!(
        "sync: evm: tasks: {} tasks found",
        jobs.evm_jobs().tasks().len()
    );
    log!(
        "sync: svm: tasks: {} tasks found",
        jobs.svm_jobs().tasks().len()
    );

    *WORKER_STATUS.exclusive() = WorkerStatus::RUNNING;

    let (send_message, receive_message) =
        mpsc::channel::<Message>(MESSAGES_CAPACITY);

    let mut signal_bus = Bus::<Signal>::new(64);

    let channel = Arc::new(Channel::new(send_message));

    runtime.block_on(async {
        let mut scheduler = Scheduler::utc();
        evm::tasks::setup(&mut scheduler).await;
        svm::tasks::setup(&mut scheduler).await;

        let evm_blocks_rx = signal_bus.add_rx();
        let evm_logs_rx = signal_bus.add_rx();

        let svm_blocks_rx = signal_bus.add_rx();
        let svm_logs_rx = signal_bus.add_rx();
        let svm_transactions_rx = signal_bus.add_rx();

        let handler = tokio::spawn(handle_message(MessageStream::new(receive_message)));

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
             _ = svm::transactions::listen(Arc::clone(&channel), svm_transactions_rx) => {
                 log!("sync: stopped listening to transactions... exiting");
             },
             _ = evm::tasks::handle_tasks(Arc::clone(&channel)) => {
                 log!("sync: tasks: stopped tasks... exiting");
             },
             _ = svm::tasks::handle_tasks(Arc::clone(&channel)) => {
                 log!("sync: tasks: stopped tasks... exiting");
             },
        }

        if channel.send(Message::Shutdown) {
          if let Err(err) = handler.await {
              log!("sync: messages: exited with error: {}", err);
          }
        }
    });

    for job in &jobs {
        let id = job.id.clone();
        BackgroundWorker::transaction(|| {
            let _ = PgTryBuilder::new(|| Job::update(id, JobStatus::Stopped))
                .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                .execute();
        });
    }

    *WORKER_STATUS.exclusive() = WorkerStatus::STOPPED;
    log!("sync: worker has exited");
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
            warning!("sync: messages: got None");
            continue;
        };

        match message {
            Message::Job(id, oneshot) => {
                let jobs = BackgroundWorker::transaction(|| {
                    PgTryBuilder::new(Job::query_all)
                        .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                        .execute()
                })
                .unwrap_or(Vec::new());

                match jobs.into_iter().find(|job| job.id == id) {
                    Some(job) => {
                        if oneshot.send(Some(job)).is_err() {
                            warning!("sync: jobs: failed to send on channel");
                        }
                    }
                    None => {
                        if oneshot.send(None).is_err() {
                            warning!("sync: jobs: failed to send on channel");
                        }
                    }
                };
            }
            Message::Jobs(oneshot) => {
                let jobs = BackgroundWorker::transaction(|| {
                    PgTryBuilder::new(Job::query_all)
                        .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                        .execute()
                })
                .unwrap_or(Vec::new());

                if oneshot.send(jobs).is_err() {
                    warning!("sync: jobs: failed to send on channel");
                }
            }
            Message::UpdateJob(status, job) => {
                let id = job.id;
                BackgroundWorker::transaction(|| {
                    PgTryBuilder::new(|| Job::update(id, status))
                        .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                        .execute()
                })
                .expect("sync: jobs: failed to update job status");
            }
            Message::EvmBlock(..) => {
                evm_blocks.fetch_add(1, Ordering::Relaxed);
                evm::blocks::handle_message(message);
            }
            Message::EvmLog(..) => {
                evm_logs.fetch_add(1, Ordering::Relaxed);
                evm::logs::handle_message(message);
            }
            Message::SvmBlock(..) => {
                svm_blocks.fetch_add(1, Ordering::Relaxed);
                svm::blocks::handle_message(message);
            }
            Message::SvmLog(..) => {
                svm_logs.fetch_add(1, Ordering::Relaxed);
                svm::logs::handle_message(message);
            }
            Message::SvmTransaction(..) => {
                svm_txs.fetch_add(1, Ordering::Relaxed);
                svm::transactions::handle_message(message)
            }
            Message::TaskSuccess(job) => {
                if let Some(handler) = job.options.success_handler.as_ref() {
                    let id = job.id;

                    BackgroundWorker::transaction(|| {
                        PgTryBuilder::new(|| Job::handler(&handler, id))
                            .catch_others(|_| {
                                Err(pgrx::spi::Error::NoTupleTable)
                            })
                            .execute()
                    })
                    .expect("sync: tasks: failed to execute success handler");
                }
            }
            Message::TaskFailure(job) => {
                if let Some(handler) = job.options.failure_handler.as_ref() {
                    let id = job.id;

                    BackgroundWorker::transaction(|| {
                        PgTryBuilder::new(|| Job::handler(&handler, id))
                            .catch_others(|_| {
                                Err(pgrx::spi::Error::NoTupleTable)
                            })
                            .execute()
                    })
                    .expect("sync: tasks: failed to execute failure handler");
                }
            }
            Message::CheckBlock(number, oneshot, job) => {
                if let Some(handler) = job.options.block_check_handler.as_ref()
                {
                    let id = job.id;
                    let found = BackgroundWorker::transaction(|| {
                        PgTryBuilder::new(|| {
                            u64::call_handler(&number, handler, id)
                        })
                        .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                        .execute()
                    })
                    .expect("sync: tasks: failed to execute success handler");

                    match found {
                        PgResult::Boolean(found) => {
                            if oneshot.send(found).is_err() {
                                warning!("sync: check_block: failed to send on channel, event job stalled");
                            }
                        }
                        _ => {
                            if oneshot.send(false).is_err() {
                                warning!("sync: check_block: failed to send on channel, event job stalled");
                            }
                        }
                    };
                }
            }
            Message::Shutdown => {
                stats.abort();
                break;
            }
        }
    }
}
