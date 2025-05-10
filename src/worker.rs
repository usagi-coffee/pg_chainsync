use pgrx::lwlock::PgLwLock;
use pgrx::PGRXSharedMemory;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::bgworkers::*;

use pgrx::GucSetting;

use crate::channel::Channel;
use std::sync::Arc;

use tokio::time::{sleep_until, Duration, Instant};

use crate::types::*;
use bus::Bus;

pub static WORKER_STATUS: PgLwLock<WorkerStatus> =
    PgLwLock::new(c"worker_status");
pub static RESTART_COUNT: PgLwLock<i32> = PgLwLock::new(c"restart_count");
pub static SIGNALS: PgLwLock<heapless::Vec<u8, 32>> = PgLwLock::new(c"signals");
pub static EVM_TASKS: PgLwLock<heapless::Vec<i64, 32>> =
    PgLwLock::new(c"evm_tasks");
pub static SVM_TASKS: PgLwLock<heapless::Vec<i64, 32>> =
    PgLwLock::new(c"svm_tasks");

pub static WS_PERMITS: GucSetting<i32> = GucSetting::<i32>::new(1);

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
