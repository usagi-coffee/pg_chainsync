use pgrx::prelude::*;
use pgrx::{
    GucContext, GucFlags, GucRegistry, PgSharedMemoryInitialization,
    pg_shmem_init,
};

#[macro_use]
pub mod worker;

pub mod channel;
pub mod query;
pub mod types;

pub mod evm;
pub mod svm;

mod sync;

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
        if let Some(cron) = &configuration.cron
            && Schedule::from_str(&cron).is_err()
        {
            panic!("incorrect cron expression")
        }

        let id = Job::register(name.into(), options);

        if matches!(configuration.evm, Some(_))
            && let Some(oneshot) = configuration.oneshot
            && oneshot
            && let Err(_) = EVM_TASKS.exclusive().push(id)
        {
            panic!("failed to enqueue the task");
        } else if matches!(configuration.svm, Some(_))
            && let Some(oneshot) = configuration.oneshot
            && oneshot
            && let Err(_) = SVM_TASKS.exclusive().push(id)
        {
            panic!("failed to enqueue the task");
        }

        // Send signal to the worker to restart the loop if it's a job
        if !matches!(configuration.oneshot, Some(true)) {
            if configuration.is_block_job()
                && SIGNALS
                    .exclusive()
                    .push(crate::types::Signal::RestartBlocks as u8)
                    .is_err()
            {
                panic!("failed to send restart signal");
            } else if configuration.is_log_job()
                && SIGNALS
                    .exclusive()
                    .push(crate::types::Signal::RestartLogs as u8)
                    .is_err()
            {
                panic!("failed to send restart signal");
            }
        }

        id
    }
}

extension_sql_file!("../sql/types.sql", name = "types_schema");

use worker::{
    DATABASE, EVM_BLOCKTICK_RESET, EVM_TASKS, EVM_WS_PERMITS, RESTART_COUNT,
    SIGNALS, SVM_RPC_PERMITS, SVM_SIGNATURES_BUFFER, SVM_TASKS, WORKER_STATUS,
};

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(WORKER_STATUS);
    pg_shmem_init!(RESTART_COUNT);
    pg_shmem_init!(EVM_TASKS);
    pg_shmem_init!(SVM_TASKS);
    pg_shmem_init!(SIGNALS);

    GucRegistry::define_string_guc(
        c"chainsync.database",
        c"database where the chainsync schema is",
        c"database where the chainsync schema is",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"chainsync.evm_ws_permits",
        c"number of permits per ws key",
        c"number of permits per ws key",
        &EVM_WS_PERMITS,
        1,
        999,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"chainsync.evm_blocktick_reset",
        c"number of range fetches before blocktick reset",
        c"number of range fetches before blocktick reset",
        &EVM_BLOCKTICK_RESET,
        1,
        999999,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"chainsync.svm_ws_permits",
        c"number of permits per rpc key in a single task",
        c"number of permits per rpc ket in a single task",
        &SVM_RPC_PERMITS,
        1,
        999,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"chainsync.svm_signatures_buffer",
        c"number of signatures to buffer in a single task",
        c"number of signatures to buffer in a single task",
        &SVM_SIGNATURES_BUFFER,
        1,
        100000000,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    worker::spawn().load();
}
