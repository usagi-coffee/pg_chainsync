use pgx::bgworkers::*;
use pgx::prelude::*;
use pgx::{pg_shmem_init, PgSharedMemoryInitialization};

mod sync;
mod worker;

pub mod query;
pub mod types;

pgx::pg_module_magic!();

#[pg_schema]
mod chainsync {
    use crate::types::{Job, JobType};
    use crate::worker::{
        WorkerStatus, RESTART_COUNT, STOP_COUNT, WORKER_STATUS,
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
    ) -> bool {
        Job::register(
            JobType::Blocks,
            chain_id,
            provider_url,
            callback,
            pgx::JsonB(serde_json::Value::Null),
        )
    }
    }
}

extension_sql_file!("../sql/types.sql", name = "types_schema");
extension_sql_file!("../sql/jobs.sql", name = "jobs_schema");

use worker::{RESTART_COUNT, WORKER_STATUS};

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(WORKER_STATUS);
    pg_shmem_init!(RESTART_COUNT);

    BackgroundWorkerBuilder::new("pg_chainsync: sync worker")
        .set_function("background_worker_sync")
        .set_library("pg_chainsync")
        .enable_spi_access()
        .load();
}
