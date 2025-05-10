use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use alloy::providers::Provider;
use alloy::transports::RpcError;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{log, warning};

use tokio::sync::{oneshot, Semaphore};
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
    let mut semaphores: HashMap<String, Arc<Semaphore>> = HashMap::new();

    loop {
        let task: Option<i64> = { EVM_TASKS.exclusive().pop() };

        if let Some(task) = task {
            log!("sync: evm: tasks: got task {}", task);

            // FIXME: wait some time for commit when adding tasks
            sleep_until(Instant::now() + Duration::from_millis(100)).await;

            let (tx, rx) = oneshot::channel::<Option<Job>>();
            channel.send(Message::Job(task, tx));

            let Ok(job) = rx.await else {
                warning!(
                    "sync: evm: tasks: failed to fetch job for task {}",
                    task
                );
                continue;
            };

            let Some(job) = job else {
                warning!(
                    "sync: evm: tasks: failed to find job for task {}",
                    task
                );
                continue;
            };

            let Some(ws) = &job.options.ws else {
                warning!(
                    "sync: evm: tasks: no ws was provided for task {}",
                    task
                );
                continue;
            };

            if let Err(err) = job.connect_evm().await {
                warning!(
                    "sync: evm: tasks: failed to create provider for {}, {}",
                    task,
                    err
                );
                continue;
            };

            let semaphore = semaphores
                .entry(ws.into())
                .or_insert(Arc::new(Semaphore::new(1)))
                .clone();

            let channel = channel.clone();
            tokio::spawn(async move {
                let Ok(permit) = semaphore.acquire().await else {
                    warning!("sync: evm: tasks: failed to acquire semaphore for task {}", task);
                    return;
                };

                log!("sync: evm: tasks: {}: permitted", &job.name);

                let job = Arc::new(job);

                if job.options.is_block_job() {
                    handle_blocks_task(Arc::clone(&job), &channel).await;
                } else if job.options.is_log_job() {
                    handle_log_task(Arc::clone(&job), &channel).await;
                } else {
                    warning!("sync: evm: tasks: unknown  task {}", task);
                }

                drop(permit);
                channel.send(Message::TaskSuccess(Arc::clone(&job)));
            });
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

struct FetchState {
    from: i64,
    to: i64,
    blocktick: i64,
    index: i64,
    retries: i32,
}

impl FetchState {
    fn new(from: i64, to: i64, blocktick: i64) -> Self {
        FetchState {
            from,
            to,
            blocktick,
            index: 0,
            retries: 0,
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
        self.retries = 0;

        self.blocktick =
            std::cmp::max((self.blocktick as f64 / 2.0).floor() as i64, 1);
    }

    fn recalculate(&mut self, from: i64, blocktick: i64) {
        self.from = from;
        self.index = 0;
        self.retries = 0;

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
    let to_block = {
        if let Some(target) = options.to_block {
            // Use latest
            if target == 0 {
                block as i64
            }
            // Offset
            else if target < 0 {
                (block as i64) + target as i64
            }
            // Specific block
            else {
                target as i64
            }
        }
        // Use latest
        else {
            block as i64
        }
    };

    let mut client = job.reconnect_evm().await.expect("ws to connect");

    // Split logs by blocktick if needed
    if let Some(blocktick) = options.blocktick {
        let mut state = FetchState::new(from_block, to_block, blocktick);

        while state.remaining() {
            let (from, to) = state.range();

            // Sometimes "busy block periods" reduce the blocktick, try to revert every now and then
            if state.index > 100 && state.blocktick != blocktick {
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
            let logs = tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => Err(RpcError::NullResp),
                logs = request => logs
            };

            match logs {
                Ok(mut logs) => {
                    state.retries = 0;
                    for log in logs.drain(0..) {
                        logs::handle_evm_log(&job, log, &channel).await;
                    }

                    state.next();
                }
                Err(e) => {
                    warning!("sync: evm: tasks: {:?}", e);
                    if state.blocktick <= 1 || state.retries >= 20 {
                        warning!(
                          "sync: evm: tasks: {}: failed to fetch with reduced blocktick, aborting...",
                          &job.name,
                      );

                        break;
                    }

                    // Reconnect the client
                    client = job.reconnect_evm().await.expect("ws to connect");
                    log!(
                        "sync: evm: tasks: {}: reducing blocktick from {} to {}",
                        &job.name,
                        state.blocktick,
                        (state.blocktick as f64 / 2.0).floor(),
                    );

                    sleep_until(Instant::now() + Duration::from_millis(200))
                        .await;

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
            Err(e) => {
                warning!("sync: evm: tasks: {}: {:?}", &job.name, e);
                warning!(
                    "sync: evm: tasks: {}: failed to get logs, aborting...",
                    &job.name,
                );
                channel.send(Message::TaskFailure(Arc::clone(&job)));
            }
        }
    }
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
