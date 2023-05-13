use pgx::prelude::*;

use pgx::bgworkers::*;
use pgx::log;

use ethers::prelude::*;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::query::{call_block_handler, call_event_handler};
use crate::types::*;
use crate::worker::{WorkerStatus, RESTART_COUNT, WORKER_STATUS};

mod blocks;
mod events;

pub type MessageSender = mpsc::Sender<Message>;
pub type MessageStream = ReceiverStream<Message>;

const DATABASE: &str = "pg_chainsync";
const MESSAGES_CAPACITY: usize = 1024;

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_sync(_arg: pg_sys::Datum) {
    // Auto-quit after n restarts, require manual restart
    if *RESTART_COUNT.exclusive() >= 5 {
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
            mpsc::channel::<Message>(MESSAGES_CAPACITY);
        let mut message_stream = MessageStream::new(receive_message);

        runtime.block_on(async {
            tokio::select! {
                _ = handle_signals() => {
                    log!("sync: received exit signal... exiting");
                },
                _ = events::listen(Arc::clone(&jobs), send_message.clone()) => {
                    log!("sync: stopped listening to events... exiting");
                },
                _ = blocks::listen(Arc::clone(&jobs), send_message.clone()) => {
                    log!("sync: stopped listening to blocks... exiting");
                },
                _ = handle_message(&mut message_stream) => {
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

async fn handle_message(stream: &mut MessageStream) {
    while let Some(message) = stream.next().await {
        match message {
            Message::Block(..) => blocks::handle_message(&message),
            Message::Event(..) => events::handle_message(&message),
            Message::CheckBlock(chain, number, callback, channel) => {
                match channel
                    .send(blocks::check_one(&chain, &number, &callback))
                {
                    Ok(..) => {}
                    Err(..) => {}
                }
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
