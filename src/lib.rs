use pgx::bgworkers::*;
use pgx::prelude::*;
use pgx::{pg_shmem_init, PgSharedMemoryInitialization};

mod sync;
mod worker;

pub mod channel;
pub mod query;
pub mod types;

pgx::pg_module_magic!();

#[pg_schema]
mod chainsync {
    use crate::types::{Job, JobKind};
    use crate::worker::{
        WorkerStatus, RESTART_COUNT, STOP_COUNT, TASKS, WORKER_STATUS,
    };

    use pgx::bgworkers::*;
    use pgx::prelude::*;

    #[pg_extern]
    fn restart() {
        *RESTART_COUNT.exclusive() = 0;

        if *WORKER_STATUS.exclusive() == WorkerStatus::STOPPED {
            BackgroundWorkerBuilder::new("pg_chainsync: sync worker")
                .set_function("background_worker_sync")
                .set_library("pg_chainsync")
                .enable_spi_access()
                .load_dynamic();
        } else {
            *WORKER_STATUS.exclusive() = WorkerStatus::RESTARTING;
        }
    }

    #[pg_extern]
    fn stop() {
        *RESTART_COUNT.exclusive() = STOP_COUNT;
        *WORKER_STATUS.exclusive() = WorkerStatus::STOPPING;
    }

    #[pg_extern]
    fn add_blocks_job(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
    ) -> i64 {
        Job::register(
            JobKind::Blocks,
            chain_id,
            provider_url,
            callback,
            false,
            pgx::JsonB(serde_json::Value::Null),
        )
    }

    #[pg_extern]
    fn add_blocks_task(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
    ) -> i64 {
        let task = Job::register(
            JobKind::Blocks,
            chain_id,
            provider_url,
            callback,
            true,
            pgx::JsonB(serde_json::Value::Null),
        );

        if task <= 0 {
            return task;
        }

        if let Err(_) = TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }

    #[pg_extern]
    fn add_events_job(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
        options: pgx::JsonB,
    ) -> i64 {
        if options.0.is_null() || !options.0.is_object() {
            panic!("provided options are not an object")
        }

        Job::register(
            JobKind::Events,
            chain_id,
            provider_url,
            callback,
            false,
            options,
        )
    }

    #[pg_extern]
    fn add_events_task(
        chain_id: i64,
        provider_url: &str,
        callback: &str,
        options: pgx::JsonB,
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
            options,
        );

        if let Err(_) = TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }
}

extension_sql_file!("../sql/types.sql", name = "types_schema");

use worker::{RESTART_COUNT, TASKS, WORKER_STATUS};

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(WORKER_STATUS);
    pg_shmem_init!(RESTART_COUNT);
    pg_shmem_init!(TASKS);

    BackgroundWorkerBuilder::new("pg_chainsync: sync worker")
        .set_function("background_worker_sync")
        .set_library("pg_chainsync")
        .enable_spi_access()
        .load();
}
