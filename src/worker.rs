use pgx::lwlock::PgLwLock;
use pgx::PGXSharedMemory;

use pgx::bgworkers::BackgroundWorker;
use pgx::{log, warning};

use crate::types::{Job, JobKind, Message};

pub static WORKER_STATUS: PgLwLock<WorkerStatus> = PgLwLock::new();
pub static RESTART_COUNT: PgLwLock<i32> = PgLwLock::new();
pub static TASKS: PgLwLock<heapless::Vec<i64, 32>> = PgLwLock::new();

// Should be more than restart count
pub static STOP_COUNT: i32 = 999;

#[derive(Debug, Copy, Clone, Default, PartialEq)]
pub enum WorkerStatus {
    #[default]
    INITIALIZING,
    RUNNING,
    RESTARTING,
    STOPPING,
    STOPPED,
}

unsafe impl PGXSharedMemory for WorkerStatus {}

use crate::channel::Channel;
use crate::sync::events;
use std::sync::Arc;

use tokio::time::{sleep_until, Duration, Instant};

pub async fn handle_signals(_: Arc<Channel>) {
    loop {
        if BackgroundWorker::sighup_received() {
            *WORKER_STATUS.exclusive() = WorkerStatus::STOPPING;
        }

        if BackgroundWorker::sigterm_received() {
            *WORKER_STATUS.exclusive() = WorkerStatus::STOPPING;
        }

        match WORKER_STATUS.share().clone() {
            WorkerStatus::RESTARTING => break,
            WorkerStatus::STOPPING => break,
            _ => {}
        }

        sleep_until(Instant::now() + Duration::from_millis(100)).await;
    }
}

use ethers::providers::Middleware;
use tokio::sync::oneshot;

pub async fn handle_tasks(channel: Arc<Channel>) {
    loop {
        if let Some(task) = TASKS.exclusive().pop() {
            log!("sync: tasks: got task {}", task);

            // FIXME: wait some time for commit when adding tasks
            sleep_until(Instant::now() + Duration::from_millis(100)).await;

            let (tx, rx) = oneshot::channel::<Option<Job>>();
            channel.send(Message::Job(task, tx));

            let job = rx.await;

            if let Err(_) = job {
                warning!("sync: tasks: failed to fetch job for task {}", task);
                continue;
            }

            let job = job.unwrap();

            if job.is_none() {
                warning!("sync: tasks: failed to find job for task {}", task);
                continue;
            }

            let mut job = job.unwrap();

            if !job.connect().await {
                warning!("sync: tasks: failed to create provider for {}", task,);
                continue;
            };

            let chain = &job.chain;

            match job.kind {
                JobKind::Blocks => {
                    let ws = job.ws.as_ref().unwrap();
                    let options = &job.options.unwrap();

                    let mut to = options.to_block.unwrap_or(0);
                    if options.to_block.is_none() {
                        to = ws.get_block_number().await.unwrap().as_u64()
                            as i64;
                    }

                    for i in options.from_block.unwrap()..to {
                        if let Ok(block) = ws.get_block(i as u64).await {
                            if let Some(block) = block {
                                channel.send(Message::Block(
                                    *chain,
                                    block,
                                    job.callback.clone(),
                                ));
                            }
                        }
                    }
                }
                JobKind::Events => {
                    let filter = events::build_filter(&job.options.unwrap());
                    let logs = job.ws.as_ref().unwrap().get_logs(&filter).await;

                    if let Ok(mut logs) = logs {
                        for log in logs.drain(0..) {
                            channel.send(Message::Event(
                                *chain,
                                log,
                                job.callback.clone(),
                            ));
                        }
                    }
                }
            }
        }

        sleep_until(Instant::now() + Duration::from_millis(250)).await;
    }
}
