use pgrx::prelude::*;
use pgrx::{pg_shmem_init, PgSharedMemoryInitialization};

mod sync;
mod tasks;
mod worker;

pub mod channel;
pub mod query;
pub mod types;

::pgrx::pg_module_magic!();

#[pg_schema]
pub mod chainsync {
    use crate::types::{Job, JobKind};
    use crate::worker;
    use crate::worker::*;

    use pgrx::prelude::*;

    use std::str::FromStr;
    use std::time::{Duration, Instant};

    use cron::Schedule;

    #[pg_extern]
    fn restart() {
        *RESTART_COUNT.exclusive() = 0;

        if *WORKER_STATUS.exclusive() == WorkerStatus::STOPPED {
            worker::spawn().load_dynamic().unwrap();
            return;
        }

        *WORKER_STATUS.exclusive() = WorkerStatus::RESTARTING;

        let start = Instant::now();
        loop {
            if *WORKER_STATUS.share() == WorkerStatus::STOPPED {
                worker::spawn().load_dynamic().unwrap();
                return;
            }

            if start.elapsed() > Duration::from_secs(30) {
                panic!("Waited too long for restart... panicing")
            }
        }
    }

    #[pg_extern]
    fn stop() {
        *RESTART_COUNT.exclusive() = STOP_COUNT;
        *WORKER_STATUS.exclusive() = WorkerStatus::STOPPING;
    }

    #[pg_extern]
    fn run_task(task: i64) -> i64 {
        if let Err(_) = TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }

    #[pg_extern]
    fn add_blocks_job(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
    ) -> i64 {
        let id = Job::register(
            JobKind::Blocks,
            chain_id,
            provider_url,
            callback,
            false,
            false,
            None,
            pgrx::JsonB(serde_json::Value::Null),
        );

        if let Err(_) = SIGNALS
            .exclusive()
            .push(crate::types::Signal::RestartBlocks as u8)
        {
            panic!("failed to send signal");
        }

        id
    }

    #[pg_extern]
    fn add_events_job(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
        options: pgrx::JsonB,
    ) -> i64 {
        if options.0.is_null() || !options.0.is_object() {
            panic!("provided options are not an object")
        }

        let id = Job::register(
            JobKind::Events,
            chain_id,
            provider_url,
            callback,
            false,
            false,
            None,
            options,
        );

        if let Err(_) = SIGNALS
            .exclusive()
            .push(crate::types::Signal::RestartEvents as u8)
        {
            panic!("failed to send signal");
        }

        id
    }

    #[pg_extern]
    fn add_events_task(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
        options: pgrx::JsonB,
    ) -> i64 {
        if options.0.is_null() || !options.0.is_object() {
            panic!("provided options are not an object")
        }

        let task = Job::register(
            JobKind::Events,
            chain_id,
            provider_url,
            callback,
            true,
            false,
            None,
            options,
        );

        if let Err(_) = TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }

    #[pg_extern]
    fn preload_events_task(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
        options: pgrx::JsonB,
    ) -> i64 {
        if options.0.is_null() || !options.0.is_object() {
            panic!("provided options are not an object")
        }

        let task = Job::register(
            JobKind::Events,
            chain_id,
            provider_url,
            callback,
            true,
            true,
            None,
            options,
        );

        if let Err(_) = TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }

    #[pg_extern]
    fn add_events_cron(
        chain_id: i64,
        provider_url: &str,
        cron: &str,
        callback: &str,
        options: pgrx::JsonB,
    ) -> i64 {
        if options.0.is_null() || !options.0.is_object() {
            panic!("provided options are not an object")
        }

        if Schedule::from_str(&cron).is_err() {
            panic!("incorrect cron expression")
        }

        let task = Job::register(
            JobKind::Events,
            chain_id,
            provider_url,
            callback,
            true,
            false,
            Some(cron),
            options,
        );

        if let Err(_) = TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }
}

extension_sql_file!("../sql/types.sql", name = "types_schema");

use worker::{RESTART_COUNT, SIGNALS, TASKS, TASKS_PRELOADED, WORKER_STATUS};

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(WORKER_STATUS);
    pg_shmem_init!(RESTART_COUNT);
    pg_shmem_init!(TASKS);
    pg_shmem_init!(TASKS_PRELOADED);
    pg_shmem_init!(SIGNALS);

    worker::spawn().load();
}
