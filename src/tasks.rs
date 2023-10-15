use std::str::FromStr;
use std::sync::Arc;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{log, warning};

use ethers::providers::Middleware;

use tokio::sync::oneshot;
use tokio::time::{sleep_until, Duration, Instant};

use cron::Schedule;
use tokio_cron::{Job as CronJob, Scheduler};

use crate::channel::Channel;
use crate::sync::events;
use crate::types::{Job, JobKind, Message};

use crate::worker::{TASKS, TASKS_PRELOADED};

pub async fn setup() {
    let preloaded = *TASKS_PRELOADED.share();

    let mut scheduler = Scheduler::utc();

    let tasks = BackgroundWorker::transaction(|| Job::query_all());
    if tasks.is_err() {
        warning!("sync: tasks: failed to setup tasks");
        return;
    }

    let tasks = tasks.unwrap();

    for task in tasks {
        if !task.oneshot {
            continue;
        }

        if let Some(options) = task.options {
            // Preload
            if !preloaded {
                if options.preload.unwrap_or(false) {
                    if TASKS.exclusive().push(task.id).is_err() {
                        warning!("sync: tasks: failed to enqueue {}", task.id);
                    }
                }
            }

            // Cron
            if let Some(cron) = options.cron {
                if Schedule::from_str(&cron).is_err() {
                    warning!(
                        "sync: tasks: task {} has incorrect cron expression {}",
                        task.id,
                        cron
                    );

                    continue;
                }

                scheduler.add(CronJob::new_sync(cron, move || {
                    for id in &*TASKS.share() {
                        if id == &task.id {
                            warning!(
                                "sync: tasks: task {} already in queue",
                                task.id
                            );
                            return;
                        }
                    }

                    if TASKS.exclusive().push(task.id).is_err() {
                        warning!("sync: tasks: failed to enqueue {}", task.id);
                    }
                }));
            }
        }
    }

    *TASKS_PRELOADED.exclusive() = true;
}

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

            if let Err(err) = job.connect().await {
                warning!("sync: tasks: failed to create provider for {}, {}", task, err);
                continue;
            };

            let chain = &job.chain;
            let options = &job.options.as_ref().unwrap();
            let ws = job.ws.as_ref().unwrap();

            match job.kind {
                JobKind::Blocks => {
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
                                    Some(job.id),
                                ));
                            }
                        }
                    }
                }
                JobKind::Events => {
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

                        for i in 1..=splits {
                            let mut from = from_block + (i - 1) * blocktick;
                            let to = std::cmp::min(to_block, from + blocktick);

                            if i > 1 {
                                from = from + 1;
                            }

                            log!(
                                "sync: tasks: {}: fetching blocks {} to {} ({} / {})",
                                task,
                                from,
                                to,
                                i,
                                splits
                            );

                            filter = filter.from_block(from).to_block(to);

                            match ws.get_logs(&filter).await {
                                Ok(mut logs) => {
                                    for log in logs.drain(0..) {
                                        events::handle_log(&job, log, &channel)
                                            .await;
                                    }
                                }
                                Err(e) => {
                                    log!("{}", e);
                                    warning!(
                                        "sync: tasks: {}: failed to fetch split {}, aborting...",
                                        task,
                                        i
                                    );

                                    break;
                                }
                            }
                        }
                    } else {
                        match ws.get_logs(&filter).await {
                            Ok(mut logs) => {
                                for log in logs.drain(0..) {
                                    events::handle_log(&job, log, &channel)
                                        .await;
                                }
                            }
                            Err(e) => {
                                log!("{}", e);
                                warning!(
                                    "sync: tasks: failed to get logs for {}, aborting...",
                                    task
                                );
                            }
                        }
                    }
                }
            }
        }

        sleep_until(Instant::now() + Duration::from_millis(250)).await;
    }
}
