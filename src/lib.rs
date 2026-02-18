use pgrx::prelude::*;
use pgrx::{
    GucContext, GucFlags, GucRegistry, PgSharedMemoryInitialization,
    pg_shmem_init,
};

#[macro_use]
pub mod worker;

pub mod channel;
pub mod config;
pub mod module_protocol;
pub mod module_runtime;
pub mod plugin;
pub mod prepared;
pub mod query;
pub mod types;

pub mod evm;
pub mod svm;

mod sync;

::pgrx::pg_module_magic!();

#[pg_schema]
mod chainsync {
    use crate::config::{self, RuntimeStatus};
    use crate::worker;
    use crate::worker::*;

    use pgrx::prelude::*;
    use serde_json::json;
    use std::time::{Duration, Instant};

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
        let _ = name;
        let _ = options;
        panic!(
            "chainsync.register is removed in v2. Define handlers under chainsync.config_dir and call chainsync.reload()"
        );
    }

    #[pg_extern]
    fn reload() -> pgrx::JsonB {
        let config_dir = CONFIG_DIR
            .get()
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .expect("chainsync.config_dir must be configured");

        let statuses = Spi::connect(|_| {
            config::sync_from_handlers(std::path::Path::new(&config_dir))
        })
        .expect("reload failed");

        if SIGNALS
            .exclusive()
            .push(crate::types::Signal::RestartBlocks as u8)
            .is_err()
        {
            warning!("failed to send block restart signal");
        }
        if SIGNALS
            .exclusive()
            .push(crate::types::Signal::RestartLogs as u8)
            .is_err()
        {
            warning!("failed to send log restart signal");
        }

        let payload = statuses
            .into_iter()
            .map(|status: RuntimeStatus| {
                json!({
                    "job_id": status.job_id,
                    "status": status.status,
                    "last_error": status.last_error,
                })
            })
            .collect::<Vec<_>>();
        pgrx::JsonB(json!(payload))
    }
}

extension_sql_file!("../sql/types.sql", name = "types_schema");

use worker::{
    CONFIG_DIR, DATABASE, EVM_BLOCKTICK_RESET, EVM_TASKS, EVM_WS_PERMITS,
    RESTART_COUNT, SIGNALS, SVM_RPC_PERMITS, SVM_SIGNATURES_BUFFER, SVM_TASKS,
    WORKER_STATUS,
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
    GucRegistry::define_string_guc(
        c"chainsync.config_dir",
        c"directory with handler folders and handler.toml files",
        c"directory with handler folders and handler.toml files",
        &CONFIG_DIR,
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
