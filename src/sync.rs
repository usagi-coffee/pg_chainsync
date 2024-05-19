use std::sync::Arc;

use pgrx::prelude::*;

use pgrx::bgworkers::*;
use pgrx::log;

use tokio::sync::mpsc;
use tokio_cron::Scheduler;
use tokio_stream::StreamExt;

use bus::Bus;

use crate::channel::*;
use crate::tasks;
use crate::types::*;
use crate::worker;
use crate::worker::*;

pub mod blocks;
pub mod events;

const DATABASE: &str = "postgres";

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

    let jobs = BackgroundWorker::transaction(|| {
        PgTryBuilder::new(Job::query_all)
            .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
            .execute()
    })
    .unwrap_or(Vec::new());

    log!("sync: {} jobs found", jobs.len());

    *WORKER_STATUS.exclusive() = WorkerStatus::RUNNING;

    let (send_message, receive_message) =
        mpsc::channel::<Message>(MESSAGES_CAPACITY);

    let mut signal_bus = Bus::<Signal>::new(64);

    let channel = Arc::new(Channel::new(send_message));
    let mut stream = MessageStream::new(receive_message);

    runtime.block_on(async {
        let mut scheduler = Scheduler::utc();
        tasks::setup(&mut scheduler).await;

        let blocks_rx = signal_bus.add_rx();
        let events_rx = signal_bus.add_rx();

        tokio::select! {
             _ = worker::handle_signals(Arc::clone(&channel), signal_bus) => {
                 log!("sync: received exit signal... exiting");
             },
             _ = blocks::listen(Arc::clone(&channel), blocks_rx) => {
                 log!("sync: stopped listening to blocks... exiting");
             },
             _ = events::listen(Arc::clone(&channel), events_rx) => {
                 log!("sync: stopped listening to events... exiting");
             },
             _ = handle_message(&mut stream) => {
                 log!("sync: stopped processing messages... exiting");
             }
             _ = tasks::handle_tasks(Arc::clone(&channel)) => {
                 log!("sync: tasks: stopped tasks... exiting");
             },
        }
    });

    *WORKER_STATUS.exclusive() = WorkerStatus::STOPPED;
    log!("sync: worker has exited");
}

async fn handle_message(stream: &mut MessageStream) {
    while let Some(message) = stream.next().await {
        match message {
            Message::Job(id, oneshot) => {
                // TODO: query_one
                let jobs = BackgroundWorker::transaction(|| {
                    PgTryBuilder::new(Job::query_all)
                        .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                        .execute()
                })
                .unwrap_or(Vec::new());

                match jobs.into_iter().find(|job| job.id == id) {
                    Some(job) => {
                        if oneshot.send(Some(job)).is_err() {
                            warning!("sync: jobs: failed to send on channel");
                        }
                    }
                    None => {
                        if oneshot.send(None).is_err() {
                            warning!("sync: jobs: failed to send on channel");
                        }
                    }
                };
            }
            Message::Jobs(oneshot) => {
                let jobs = BackgroundWorker::transaction(|| {
                    PgTryBuilder::new(Job::query_all)
                        .catch_others(|_| Err(pgrx::spi::Error::NoTupleTable))
                        .execute()
                })
                .unwrap_or(Vec::new());

                if oneshot.send(jobs).is_err() {
                    warning!("sync: jobs: failed to send on channel");
                }
            }
            Message::Block(..) => blocks::handle_message(&message),
            Message::Event(..) => events::handle_message(&message),
            Message::CheckBlock(chain, number, callback, oneshot) => {
                if oneshot
                    .send(blocks::check_one(&chain, &number, &callback))
                    .is_err()
                {
                    warning!("sync: check_block: failed to send on channel, event job stalled");
                }
            }
        }
    }
}
