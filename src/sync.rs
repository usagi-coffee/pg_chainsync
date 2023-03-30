use pgx::prelude::*;

use pgx::bgworkers::*;
use pgx::log;

use ethers::prelude::*;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::query::call_block_handler;
use crate::types::*;
use crate::worker::{WorkerStatus, RESTART_COUNT, WORKER_STATUS};

mod blocks;

pub type MessageSender = mpsc::Sender<(Chain, Message)>;
pub type MessageStream = ReceiverStream<(Chain, Message)>;

const DATABASE: &str = "pg_chainsync";
const MESSAGES_CAPACITY: usize = 200;

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_sync(_arg: pg_sys::Datum) {
    // Auto-quit after x restarts, require manual restart
    if *RESTART_COUNT.exclusive() > 5 {
        return;
    }

    *RESTART_COUNT.exclusive() += 1;

    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );

    BackgroundWorker::connect_worker_to_spi(Some(DATABASE), None);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("sync: Failed to create async runtime");

    log!("sync: worker has started!");

    let mut jobs = BackgroundWorker::transaction(|| {
        PgTryBuilder::new(Job::query_all)
            .catch_others(|_| Err(pgx::spi::Error::NoTupleTable))
            .execute()
    })
    .unwrap_or(Vec::new());

    log!("sync: {} jobs found", jobs.len());
    for job in jobs.iter_mut() {
        let provider = runtime.block_on(async {
            Provider::<Ws>::connect(&job.provider_url).await
        });

        if provider.is_ok() {
            job.ws = Some(provider.unwrap());
        }
    }

    let jobs = Arc::new(jobs);

    if jobs.len() > 0 {
        *WORKER_STATUS.exclusive() = WorkerStatus::RUNNING;

        let (send_message, receive_message) =
            mpsc::channel::<(Chain, Message)>(MESSAGES_CAPACITY);
        let mut message_stream = MessageStream::new(receive_message);

        runtime.block_on(async {
            tokio::select! {
                _ = handle_signals() => {
                    log!("sync: received exit signal... exiting");
                },
                _ = blocks::listen(jobs.clone(), send_message.clone()) => {
                    log!("sync: stopped listening to blocks... exiting");
                },
                _ = handle_message(jobs.clone(), &mut message_stream) => {
                    log!("sync: stopped processing messages... exiting");
                }
            }
        });
    } else {
        log!("sync: no jobs found... exiting");
    }

    *WORKER_STATUS.exclusive() = WorkerStatus::STOPPED;
    log!("sync: worker has exited");
}

async fn handle_signals() {
    loop {
        if BackgroundWorker::sighup_received() {
            break;
        }

        match WORKER_STATUS.share().clone() {
            WorkerStatus::RESTARTING => return,
            _ => {}
        }

        tokio::task::yield_now().await;
    }
}

async fn handle_message(jobs: Arc<Vec<Job>>, stream: &mut MessageStream) {
    let mut job_callbacks = HashMap::new();

    for job in jobs.iter() {
        if !job_callbacks.contains_key(&job.job_type) {
            job_callbacks.insert(job.job_type, Vec::new());
        }

        // SAFETY: block above ensures that key exists
        job_callbacks
            .get_mut(&job.job_type)
            .unwrap()
            .push(job.callback.clone());
    }

    let block_callbacks = job_callbacks.get(&JobType::Blocks).unwrap();

    while let Some(tick) = stream.next().await {
        let (chain, message) = tick;

        match message {
            Message::Block(block) => {
                let number = block.number.unwrap_or_default();
                log!("sync: blocks: {}: adding {}", chain, number);

                BackgroundWorker::transaction(|| {
                    PgTryBuilder::new(|| {
                        for callback in block_callbacks.iter() {
                            call_block_handler(callback, &block)
                                .expect("failed to call the handler")
                        }
                    })
                    .catch_others(|_| {
                        error!(
                            "sync: blocks: handler failed to put block {}",
                            number
                        );
                    })
                    .catch_rust_panic(|_| {
                        error!(
                            "sync: blocks: failed to call handler for {}",
                            number
                        );
                    })
                    .execute();
                });
            }
        }
    }
}

pub async fn wait_for_messages(send_event: MessageSender) {
    loop {
        if send_event.capacity() >= MESSAGES_CAPACITY {
            break;
        }

        tokio::task::yield_now().await;
    }
}
