use pgx::lwlock::PgLwLock;
use pgx::PGXSharedMemory;

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
