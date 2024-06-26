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

pub async fn setup(scheduler: &mut Scheduler) {
    let preloaded = *TASKS_PRELOADED.share();

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

        // Preload
        if !preloaded {
            if task.preload {
                if TASKS.exclusive().push(task.id).is_err() {
                    warning!("sync: tasks: failed to enqueue {}", task.id);
                }
            }
        }

        // Cron
        if let Some(cron) = task.cron {
            if Schedule::from_str(&cron).is_err() {
                warning!(
                    "sync: tasks: task {} has incorrect cron expression {}",
                    task.id,
                    cron
                );

                continue;
            }

            scheduler.add(CronJob::new_sync(cron, move || {
                if TASKS.exclusive().push(task.id).is_err() {
                    warning!("sync: tasks: failed to enqueue {}", task.id);
                }
            }));
        }
    }

    *TASKS_PRELOADED.exclusive() = true;
}

pub async fn handle_tasks(channel: Arc<Channel>) {
    loop {
        let task: Option<i64> = { TASKS.exclusive().pop() };

        if let Some(task) = task {
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
                warning!(
                    "sync: tasks: failed to create provider for {}, {}",
                    task,
                    err
                );
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
                        let recalculate_splits =
                            |from: i64, to: i64, blocktick: i64| {
                                ((to - from) as f64 / blocktick as f64).ceil()
                                    as i64
                            };

                        let mut current_blocktick = blocktick;
                        let mut current_from = from_block;
                        let mut splits =
                            recalculate_splits(from_block, to_block, blocktick);

                        log!("sync: tasks: {}: found {} splits", task, splits);

                        let mut retries = 0;
                        let mut i = 1;
                        while i <= splits {
                            let mut from =
                                current_from + (i - 1) * current_blocktick;
                            let to = std::cmp::min(
                                to_block,
                                from + current_blocktick,
                            );

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
                                    retries = 0;
                                    for log in logs.drain(0..) {
                                        events::handle_log(&job, log, &channel)
                                            .await;
                                    }
                                }
                                Err(e) => {
                                    println!("{}", e);
                                    if current_blocktick <= 1 || retries >= 20 {
                                        warning!(
                                            "sync: tasks: {}: failed to fetch with reduced blocktick, aborting...",
                                            task,
                                        );

                                        break;
                                    }

                                    log!(
                                        "sync: tasks: {}: reducing blocktick from {} to {}",
                                        task,
                                        current_blocktick,
                                        (current_blocktick as f64 / 2 as f64).floor(),
                                    );

                                    sleep_until(
                                        Instant::now()
                                            + Duration::from_millis(200),
                                    )
                                    .await;

                                    current_blocktick =
                                        (current_blocktick as f64 / 2 as f64)
                                            .floor()
                                            as i64;
                                    current_from = from;
                                    splits = recalculate_splits(
                                        current_from,
                                        std::cmp::min(
                                            to_block,
                                            current_from + current_blocktick,
                                        ),
                                        current_blocktick,
                                    );
                                    retries = 0;
                                    i = 1;
                                    continue;
                                }
                            }

                            i = i + 1;
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
