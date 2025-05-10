use pgrx::prelude::*;
use pgrx::{
    pg_shmem_init, GucContext, GucFlags, GucRegistry,
    PgSharedMemoryInitialization,
};

mod sync;

#[macro_use]
pub mod worker;

pub mod channel;
pub mod query;
pub mod types;

::pgrx::pg_module_magic!();

#[pg_schema]
mod chainsync {
    use crate::types::{Job, JobOptions};
    use crate::worker;
    use crate::worker::*;

    use pgrx::prelude::*;
    use serde::Deserialize;

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
    fn run_evm_task(task: i64) -> i64 {
        if let Err(_) = EVM_TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }

    #[pg_extern]
    fn run_svm_task(task: i64) -> i64 {
        if let Err(_) = SVM_TASKS.exclusive().push(task) {
            panic!("failed to enqueue the task")
        }

        task
    }

    #[pg_extern]
    fn register(name: &str, options: pgrx::JsonB) -> i64 {
        if options.0.is_null() || !options.0.is_object() {
            panic!("provided options are not an object")
        }

        // Deserialize options
        let configuration = JobOptions::deserialize(&options.0)
            .expect("Invalid options provided");

        // Validate cron expression
        if let Some(cron) = &configuration.cron {
            if Schedule::from_str(&cron).is_err() {
                panic!("incorrect cron expression")
            }
        }

        let id = Job::register(name.into(), options);

        // Automatically enqueue the task if it's a one-shot job
        if let Some(oneshot) = configuration.oneshot {
            if oneshot {
                if let Err(_) = EVM_TASKS.exclusive().push(id) {
                    panic!("failed to enqueue the task")
                }
            }
        }

        // Send signal to the worker to restart the loop
        if configuration.is_block_job() {
            if let Err(_) = SIGNALS
                .exclusive()
                .push(crate::types::Signal::RestartBlocks as u8)
            {
                panic!("failed to send restart signal");
            }
        } else if configuration.is_log_job() {
            if let Err(_) = SIGNALS
                .exclusive()
                .push(crate::types::Signal::RestartLogs as u8)
            {
                panic!("failed to send restart signal");
            }
        }

        id
    }
}

extension_sql_file!("../sql/types.sql", name = "types_schema");

use worker::{
    EVM_TASKS, RESTART_COUNT, SIGNALS, SVM_TASKS, WORKER_STATUS, WS_PERMITS,
};

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(WORKER_STATUS);
    pg_shmem_init!(RESTART_COUNT);
    pg_shmem_init!(EVM_TASKS);
    pg_shmem_init!(SVM_TASKS);
    pg_shmem_init!(SIGNALS);

    GucRegistry::define_int_guc(
        "chainsync.ws_permits",
        "number of permits per ws key",
        "number of permits per ws key",
        &WS_PERMITS,
        1,
        999,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    worker::spawn().load();
}
