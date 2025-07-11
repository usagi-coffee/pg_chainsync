use pgrx::GucSetting;
use pgrx::PGRXSharedMemory;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::bgworkers::*;
use pgrx::lwlock::PgLwLock;

use std::ffi::CString;
use std::sync::Arc;

use tokio::time::{Duration, Instant, sleep_until};

use bus::Bus;

use crate::channel::Channel;
use crate::types::*;

pub static WORKER_STATUS: PgLwLock<WorkerStatus> =
    PgLwLock::new(c"worker_status");
pub static RESTART_COUNT: PgLwLock<i32> = PgLwLock::new(c"restart_count");
pub static SIGNALS: PgLwLock<heapless::Vec<u8, 32>> = PgLwLock::new(c"signals");
pub static EVM_TASKS: PgLwLock<heapless::Vec<i64, 32>> =
    PgLwLock::new(c"evm_tasks");
pub static SVM_TASKS: PgLwLock<heapless::Vec<i64, 32>> =
    PgLwLock::new(c"svm_tasks");

pub static DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));
pub static EVM_WS_PERMITS: GucSetting<i32> = GucSetting::<i32>::new(1);
pub static EVM_BLOCKTICK_RESET: GucSetting<i32> = GucSetting::<i32>::new(100);
pub static SVM_RPC_PERMITS: GucSetting<i32> = GucSetting::<i32>::new(3);
pub static SVM_SIGNATURES_BUFFER: GucSetting<i32> =
    GucSetting::<i32>::new(50000);

// Should be more than restart count
pub static STOP_COUNT: i32 = 999;
pub static RESTART_TIME: u64 = 3000;

#[derive(Debug, Copy, Clone, Default, PartialEq)]
pub enum WorkerStatus {
    #[default]
    INITIALIZING,
    RUNNING,
    RESTARTING,
    STOPPING,
    STOPPED,
}

unsafe impl PGRXSharedMemory for WorkerStatus {}

pub fn spawn() -> BackgroundWorkerBuilder {
    BackgroundWorkerBuilder::new("pg_chainsync: sync worker")
        .set_function("background_worker_sync")
        .set_library("pg_chainsync")
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_millis(RESTART_TIME)))
}

pub async fn handle_signals(_: Arc<Channel>, mut bus: Bus<Signal>) {
    loop {
        if BackgroundWorker::sighup_received() {
            *WORKER_STATUS.exclusive() = WorkerStatus::STOPPING;
        }

        if BackgroundWorker::sigterm_received() {
            *WORKER_STATUS.exclusive() = WorkerStatus::STOPPING;
        }

        match WORKER_STATUS.share().clone() {
            WorkerStatus::RESTARTING => break,
            WorkerStatus::STOPPING => break,
            _ => {}
        }

        let signal = { SIGNALS.exclusive().pop() };
        if let Some(signal) = signal {
            bus.broadcast(signal.into());
        }

        sleep_until(Instant::now() + Duration::from_millis(100)).await;
    }
}

#[macro_export]
macro_rules! anyhow_pg_try {
    ($expr:expr) => {
        BackgroundWorker::transaction(|| {
            PgTryBuilder::new($expr)
                .catch_others(|e| Err(anyhow::anyhow!(format!("{:?}", e))))
                .catch_rust_panic(|e| Err(anyhow::anyhow!(format!("{:?}", e))))
                .execute()
        })
    };
}
