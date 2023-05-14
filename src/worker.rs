use pgx::lwlock::PgLwLock;
use pgx::PGXSharedMemory;

use pgx::bgworkers::BackgroundWorker;

pub static WORKER_STATUS: PgLwLock<WorkerStatus> = PgLwLock::new();
pub static RESTART_COUNT: PgLwLock<i32> = PgLwLock::new();

// Should be more than restart count
pub static STOP_COUNT: i32 = 999;

#[derive(Debug, Copy, Clone, Default, PartialEq)]
pub enum WorkerStatus {
    #[default]
    INITIALIZING,
    RUNNING,
    RESTARTING,
    STOPPING,
    STOPPED,
}

unsafe impl PGXSharedMemory for WorkerStatus {}

pub async fn handle_signals() {
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

        tokio::task::yield_now().await;
    }
}
