use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;

use alloy::providers::Provider;
use alloy::rpc::types::Log;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{log, warning, JsonB, PgTryBuilder};

use tokio::sync::{oneshot, Semaphore};
use tokio::time::{sleep_until, Duration, Instant};
use tokio_cron::{Job as CronJob, Scheduler};

use chrono::Utc;
use cron::Schedule;

use crate::anyhow_pg_try;
use crate::channel::Channel;
use crate::evm::logs;
use crate::types::*;
use crate::worker::{EVM_BLOCKTICK_RESET, EVM_TASKS, EVM_WS_PERMITS};

pub async fn setup(scheduler: &mut Scheduler) {
    let Ok(jobs) = anyhow_pg_try!(|| Job::query_all()) else {
        warning!("sync: evm: tasks: failed to setup tasks");
        return;
    };

    let tasks = jobs.evm_jobs().tasks().into_iter();
    log!("sync: evm: tasks: {} tasks found", tasks.len());

    for task in tasks {
        let id = task.id;

        // Enqueue preloaded tasks
        if matches!(task.options.preload, Some(true)) {
            if EVM_TASKS.exclusive().push(task.id).is_err() {
                warning!("sync: evm: tasks: {}: failed to enqueue", &task.name);
            }
        }

        // Cron tasks
        if let Some(cron) = &task.options.cron {
            let Ok(schedule) = Schedule::from_str(&cron) else {
                warning!(
                    "sync: evm: tasks: {}: has incorrect cron expression {}",
                    task.name,
                    &cron
                );

                continue;
            };

            log!(
                "sync: evm: tasks: {}: scheduling {}, next at {}",
                &task.name,
                &cron,
                schedule.upcoming(Utc).next().unwrap()
            );

            scheduler.add(CronJob::new_sync(cron, move || {
                if EVM_TASKS.exclusive().push(id).is_err() {
                    warning!("sync: evm: tasks: failed to enqueue {}", id);
                }
            }));
        }
    }
}

pub async fn handle_tasks(channel: Arc<Channel>) {
    let mut semaphores: HashMap<String, Arc<Semaphore>> = HashMap::new();

    loop {
        let task: Option<i64> = { EVM_TASKS.exclusive().pop() };

        if let Some(task) = task {
            log!("sync: evm: tasks: got task {}", task);

            // FIXME: wait some time for commit when adding tasks
            sleep_until(Instant::now() + Duration::from_millis(250)).await;

            let (tx, rx) = oneshot::channel::<Option<Job>>();
            channel.send(Message::Job(task, tx));

            let Ok(job) = rx.await else {
                warning!(
                    "sync: evm: tasks: failed to fetch job for task {}",
                    task
                );
                continue;
            };

            let Some(mut job) = job else {
                warning!(
                    "sync: evm: tasks: failed to find job for task {}",
                    task
                );
                continue;
            };

            if let Some(setup_handler) = job.options.setup_handler.as_ref() {
                let (tx, rx) = oneshot::channel::<Option<JsonB>>();
                channel.send(Message::JsonHandler(
                    task,
                    setup_handler.to_owned(),
                    tx,
                ));

                match rx.await {
                    Ok(Some(json)) => match serde_json::from_value(json.0) {
                        // Update options
                        Ok(options) => job.options = options,
                        Err(error) => {
                            warning!("sync: evm: tasks: {}: failed to parse return handler options jsonb with {}", &job.name, &error);
                            continue;
                        }
                    },
                    Err(error) => {
                        warning!("sync: evm: tasks: {}: setup handler failed with {}", &job.name, error);
                        continue;
                    }
                    _ => {
                        warning!("sync: evm: tasks: {}: setup handler did not return options jsonb", &job.name);
                        continue;
                    }
                }
            }

            let Some(ws) = &job.options.ws else {
                warning!("sync: evm: tasks: {}: no ws was provided", &job.name);
                continue;
            };

            let channel = channel.clone();
            let semaphore = semaphores
                .entry(ws.into())
                .or_insert(Arc::new(Semaphore::new(
                    EVM_WS_PERMITS.get() as usize
                )))
                .clone();

            let job = Arc::new(job);
            tokio::spawn(async move {
                let Ok(permit) = semaphore.acquire().await else {
                    warning!(
                        "sync: evm: tasks: {}: failed to acquire semaphore",
                        &job.name
                    );
                    return;
                };

                log!("sync: evm: tasks: {}: permitted", &job.name);

                let task = {
                    if job.options.is_block_job() {
                        handle_blocks_task(job.clone(), &channel).await
                    } else if job.options.is_log_job() {
                        handle_log_task(job.clone(), &channel).await
                    } else {
                        Err(anyhow!("unknown task"))
                    }
                };

                drop(permit);

                match task {
                    Ok(()) => {
                        log!("sync: evm: tasks: {}: task completed", &job.name);

                        if let Some(success_handler) =
                            &job.options.success_handler
                        {
                            let (tx, rx) = oneshot::channel::<bool>();
                            channel.send(Message::Handler(
                                job.id,
                                success_handler.to_owned(),
                                tx,
                            ));

                            let _ = rx.await;
                        }
                    }
                    Err(error) => {
                        warning!(
                            "sync: evm: tasks: {}: task failed with {}",
                            &job.name,
                            error
                        );

                        if let Some(failure_handler) =
                            &job.options.failure_handler
                        {
                            let (tx, rx) = oneshot::channel::<bool>();
                            channel.send(Message::Handler(
                                job.id,
                                failure_handler.to_owned(),
                                tx,
                            ));

                            let _ = rx.await;
                        }
                    }
                };
            });
        }

        sleep_until(Instant::now() + Duration::from_millis(250)).await;
    }
}

async fn handle_blocks_task(
    job: Arc<Job>,
    channel: &Arc<Channel>,
) -> Result<(), anyhow::Error> {
    let options = &job.options;

    let mut client = job.reconnect_evm().await?;

    let mut from = options.from_block.unwrap_or(0);
    let to = {
        if let Some(to_block) = options.to_block {
            to_block
        } else {
            client.get_block_number().await? as i64
        }
    };

    let mut retries = 0;
    'blocks: loop {
        if retries >= 10 {
            return Err(anyhow!(
                "failed to get block after 10 retries, aborting..."
            ));
        }

        client = job.reconnect_evm().await?;

        for i in from..to {
            let block = match client
                .get_block(
                    (i as u64).into(),
                    // TODO: add configuration option for full block, for now only hashes
                    alloy::rpc::types::BlockTransactionsKind::Hashes,
                )
                .await
            {
                Ok(block) => block,
                Err(error) => {
                    warning!(
                      "sync: evm: blocks: {}: failed to get block with {}, reconnecting...",
                      &job.name,
                      error
                    );
                    retries += 1;

                    sleep_until(Instant::now() + Duration::from_millis(250))
                        .await;
                    continue 'blocks;
                }
            };

            retries = 0;

            if let Some(block) = block {
                channel.send(Message::EvmBlock(block.header, Arc::clone(&job)));
            } else {
                warning!(
                    "sync: evm: blocks: {}: got empty block at {}",
                    &job.name,
                    i
                );
            }

            from = i;
        }

        break;
    }

    Ok(())
}

struct FetchState {
    from: i64,
    to: i64,
    blocktick: i64,
    index: i64,
}

impl FetchState {
    fn new(from: i64, to: i64, blocktick: i64) -> Self {
        FetchState {
            from,
            to,
            blocktick,
            index: 0,
        }
    }

    fn range(&self) -> (i64, i64) {
        let from = self.from + self.index * self.blocktick;
        let to = {
            if from == self.to {
                self.to
            } else {
                std::cmp::min(self.to, from + self.blocktick - 1)
            }
        };

        (from, to)
    }

    fn next(&mut self) {
        self.index += 1;
    }

    fn remaining(&self) -> bool {
        self.index < self.splits()
    }

    fn reduce_blocktick(&mut self, from: i64) {
        self.from = from;
        self.index = 0;

        self.blocktick =
            std::cmp::max((self.blocktick as f64 / 2.0).floor() as i64, 1);
    }

    fn recalculate(&mut self, from: i64, blocktick: i64) {
        self.from = from;
        self.index = 0;

        self.blocktick = std::cmp::max(blocktick, 1);
    }

    fn splits(&self) -> i64 {
        if self.to == self.from {
            1
        } else {
            (((self.to + 1) - self.from) as f64 / self.blocktick as f64).ceil()
                as i64
        }
    }
}

async fn handle_log_task(
    job: Arc<Job>,
    channel: &Arc<Channel>,
) -> Result<(), anyhow::Error> {
    let options = &job.options;
    let mut client = job.reconnect_evm().await?;

    let latest = client.get_block_number().await?;
    let mut filter = logs::build_filter(options, latest);

    let from_block = options.from_block.unwrap_or(0);
    let to_block = {
        if let Some(target) = options.to_block {
            // Use latest
            if target == 0 {
                latest as i64
            }
            // Offset
            else if target < 0 {
                (latest as i64) + target as i64
            }
            // Specific block
            else {
                target as i64
            }
        }
        // Use latest
        else {
            latest as i64
        }
    };

    let reset = EVM_BLOCKTICK_RESET.get() as i64;

    // Split logs by blocktick if needed
    if let Some(blocktick) = options.blocktick {
        let mut state = FetchState::new(from_block, to_block, blocktick);
        let mut retries = 0;

        while state.remaining() {
            let (from, to) = state.range();

            // Sometimes "busy block periods" reduce the blocktick, try to revert every now and then
            if state.index >= reset && state.blocktick != blocktick {
                state.recalculate(from, blocktick);
                log!(
                    "sync: evm: tasks: {}: re-trying the original blocktick...",
                    &job.name
                );

                continue;
            }

            log!(
                "sync: evm: tasks: {}: fetching blocks {} to {} ({} / {})",
                &job.name,
                from,
                to,
                state.index + 1,
                state.splits()
            );

            filter = filter.from_block(from as u64).to_block(to as u64);
            let request = client.get_logs(&filter);

            // Do 10 second timeout to ensure we don't block forever and force-reduce blocktick
            let logs: Result<Vec<Log>, anyhow::Error> = tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => Err(anyhow!("logs request took too long")),
                logs = request => logs.map_err(|e| e.into())
            };

            match logs {
                Ok(mut logs) => {
                    retries = 0;

                    for log in logs.drain(0..) {
                        logs::handle_evm_log(&job, log, &channel).await;
                    }

                    state.next();
                }
                Err(error) => {
                    warning!(
                        "sync: evm: tasks: {}: failed to get logs with {}",
                        &job.name,
                        error
                    );

                    // Once we hit blocktick of 1 this is too slow at this point
                    if state.blocktick <= 1 {
                        return Err(anyhow!("blocktick was reduced too much"));
                    }

                    // Somethings wrong if we hit 20 retries in a row....
                    if retries >= 20 {
                        return Err(anyhow!("too many retries"));
                    }

                    // Reconnect loop
                    'reconnect: loop {
                        sleep_until(
                            Instant::now() + Duration::from_millis(200),
                        )
                        .await;

                        match job.reconnect_evm().await {
                            Ok(reconnected) => {
                                client = reconnected;
                                break 'reconnect;
                            }
                            Err(error) => {
                                warning!("sync: evm: tasks: {}: failed to reconnect with {}", &job.name, error);
                                retries += 1;
                            }
                        }
                    }

                    log!(
                        "sync: evm: tasks: {}: reducing blocktick from {} to {}",
                        &job.name,
                        state.blocktick,
                        (state.blocktick as f64 / 2.0).floor(),
                    );

                    state.reduce_blocktick(from);
                }
            }
        }
    }
    // Or just get all logs at once
    else {
        match client.get_logs(&filter).await {
            Ok(mut logs) => {
                for log in logs.drain(0..) {
                    logs::handle_evm_log(&job, log, &channel).await;
                }
            }
            Err(error) => {
                warning!(
                    "sync: evm: tasks: {}: failed to get logs with {}",
                    &job.name,
                    error
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_edge() {
        let mut s = FetchState::new(1, 1, 10);
        assert_eq!(s.index, 0);
        assert_eq!(s.blocktick, 10);
        assert_eq!(s.splits(), 1);
        assert_eq!(s.range(), (1, 1));

        while s.remaining() {
            s.next();
        }

        assert_eq!(s.index, 1);
    }

    #[test]
    fn splits_no_reduce_clamp() {
        let mut s = FetchState::new(1, 50, 10);
        assert_eq!(s.splits(), 5);

        let mut ranges = Vec::new();
        while s.remaining() {
            ranges.push(s.range());
            s.next();
        }

        assert_eq!(
            ranges,
            vec![(1, 10), (11, 20), (21, 30), (31, 40), (41, 50)]
        );
    }

    #[test]
    fn splits_reduce_once() {
        let mut s = FetchState::new(1, 30, 10);
        assert_eq!(s.splits(), 3);

        let mut ranges = Vec::new();
        ranges.push(s.range()); // (1, 10)
        s.next();

        assert_eq!(ranges[0], (1, 10));

        let current = s.range();
        assert_eq!(current, (11, 20));

        s.reduce_blocktick(current.0);
        assert_eq!(s.index, 0);

        while s.remaining() {
            ranges.push(s.range());
            s.next();
        }

        assert_eq!(
            ranges,
            vec![(1, 10), (11, 15), (16, 20), (21, 25), (26, 30)]
        );
    }

    #[test]
    fn splits_reduce_twice() {
        let mut s = FetchState::new(1, 17, 10);
        assert_eq!(s.splits(), 2);

        let mut ranges = Vec::new();
        ranges.push(s.range());
        assert_eq!(ranges[0], (1, 10));

        s.next();

        s.reduce_blocktick(s.range().0);
        s.reduce_blocktick(s.range().0);
        assert_eq!(s.index, 0);

        while s.remaining() {
            ranges.push(s.range());
            s.next();
        }

        assert_eq!(
            ranges,
            vec![(1, 10), (11, 12), (13, 14), (15, 16), (17, 17)]
        );
    }

    #[test]
    fn splits_revert() {
        let mut s = FetchState::new(1, 30, 10);

        let mut ranges = Vec::new();
        ranges.push(s.range()); // (1, 10)
        s.next();

        s.reduce_blocktick(s.range().0);
        ranges.push(s.range());
        s.next();

        // Revert
        s.recalculate(s.range().0, 10);

        while s.remaining() {
            ranges.push(s.range());
            s.next();
        }

        assert_eq!(ranges, vec![(1, 10), (11, 15), (16, 25), (26, 30)]);
    }
}
