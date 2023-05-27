use pgrx::lwlock::PgLwLock;
use pgrx::PGRXSharedMemory;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{log, warning};

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

unsafe impl PGRXSharedMemory for WorkerStatus {}

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
                    let ws = job.ws.as_ref().unwrap();
                    let options = &job.options.unwrap();

                    let mut filter = events::build_filter(options);

                    let from_block = options.from_block.unwrap_or(0);
                    let mut to_block = options.to_block.unwrap_or(0);
                    if options.to_block.is_none() {
                        to_block = ws.get_block_number().await.unwrap().as_u64()
                            as i64;
                    }

                    if let Some(blocktick) = options.blocktick {
                        let splits = ((to_block - from_block) as f64
                            / blocktick as f64)
                            .ceil() as i64;

                        log!("sync: tasks: {}: found {} splits", task, splits);

                        for i in 1..splits {
                            let mut from = from_block + (i - 1) * blocktick;
                            let to = std::cmp::min(to_block, from + blocktick);

                            if i > 1 {
                                from = from + 1;
                            }

                            filter = filter.from_block(from).to_block(to);

                            log!(
                                "sync: tasks: {}: fetching blocks {} to {}",
                                task,
                                from,
                                to
                            );

                            let logs = job
                                .ws
                                .as_ref()
                                .unwrap()
                                .get_logs(&filter)
                                .await;

                            if let Ok(mut logs) = logs {
                                for log in logs.drain(0..) {
                                    channel.send(Message::Event(
                                        *chain,
                                        log,
                                        job.callback.clone(),
                                    ));
                                }
                            } else {
                                log!(
                                    "sync: tasks: {}: failed to fetch split {}, aborting...",
                                    task,
                                    i
                                );
                                break;
                            }
                        }
                    } else {
                        let logs =
                            job.ws.as_ref().unwrap().get_logs(&filter).await;

                        if let Err(_) = logs {
                            log!(
                                "sync: tasks: failed to get logs for {}",
                                task
                            );

                            continue;
                        }

                        let mut logs = logs.unwrap();
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
