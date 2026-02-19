use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::prelude::*;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_stream::StreamExt;

use bus::Bus;

use crate::channel::*;
use crate::config;
use crate::evm;
use crate::module_protocol;
use crate::module_runtime;
use crate::prepared;
use crate::svm;
use crate::types::*;
use crate::worker;
use crate::worker::*;
use alloy::core::hex;
use serde_json::json;

const HANDLER_LANE_QUEUE_CAPACITY: usize = 50_000;
const CRON_POLL_MS: u64 = 250;

fn resolve_prefetched(
    handler: &Arc<HandlerRuntime>,
) -> Result<Option<serde_json::Value>, anyhow::Error> {
    let handler_id = handler.id;
    let Some(enrich) = &handler.options.enrich else {
        return Ok(None);
    };
    if enrich.is_empty() {
        return Ok(None);
    }

    let mut values = serde_json::Map::new();
    for lookup_id in enrich {
        let lookup_id_ref = lookup_id.as_str();
        let value = anyhow_pg_try!(|| prepared::execute_lookup(
            handler_id,
            lookup_id_ref
        ))
        .map_err(|error| {
            anyhow::anyhow!(
                "prefetch lookup '{}' failed for handler {}: {}",
                lookup_id,
                handler_id,
                error
            )
        })?;
        values.insert(lookup_id.clone(), value);
    }

    Ok(Some(serde_json::Value::Object(values)))
}

fn invoke_handler_module(
    handler: &Arc<HandlerRuntime>,
    mut payload: serde_json::Value,
) -> Result<(), anyhow::Error> {
    if let Some(prefetched) = resolve_prefetched(handler)?
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("prefetched".into(), prefetched);
    }
    if let Some(state_path) = &handler.options.state_path
        && let Some(object) = payload.as_object_mut()
    {
        object.insert(
            "state_path".into(),
            serde_json::Value::String(state_path.clone()),
        );
    }
    if let Some(log_path) = &handler.options.log_path
        && let Some(object) = payload.as_object_mut()
    {
        object.insert(
            "log_path".into(),
            serde_json::Value::String(log_path.clone()),
        );
    }
    if let Some(plugin) = &handler.options.plugin
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("plugin".into(), plugin.clone());
    }

    let bytes = serde_json::to_vec(&payload)?;
    let out = module_runtime::invoke(handler, &bytes)?;
    let response = module_protocol::decode_response(&out)?;

    match response {
        module_protocol::ModuleResponse::Ignore => Ok(()),
        module_protocol::ModuleResponse::Error { message } => {
            anyhow::bail!("module returned error: {}", message);
        }
        module_protocol::ModuleResponse::Done { mutations } => {
            let handler_id = handler.id;
            for mutation in mutations {
                let mutation_id = mutation.id;
                let payload = module_protocol::payload_to_jsonb(mutation.payload)
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "mutation '{}' payload encode failed for handler {}: {}",
                            mutation_id,
                            handler_id,
                            error
                        )
                    })?;
                anyhow_pg_try!(|| {
                    prepared::execute_mutation(
                        handler_id,
                        &mutation_id,
                        payload,
                    )
                })
                .map_err(|error| {
                    anyhow::anyhow!(
                        "mutation '{}' failed for handler {}: {}",
                        mutation_id,
                        handler_id,
                        error
                    )
                })?;
            }
            Ok(())
        }
    }
}

struct LaneJob {
    handler: Arc<HandlerRuntime>,
    payload: serde_json::Value,
}

async fn process_handler_lane(
    handler_id: i64,
    mut rx: mpsc::Receiver<LaneJob>,
) {
    while let Some(job) = rx.recv().await {
        if let Err(error) = invoke_handler_module(&job.handler, job.payload) {
            warning!(
                "sync: router: route {} module invocation failed: {}",
                handler_id,
                error
            );
        }
    }
}

fn route_to_handler_lane(
    lanes: &mut HashMap<i64, (mpsc::Sender<LaneJob>, JoinHandle<()>)>,
    handler: Arc<HandlerRuntime>,
    payload: serde_json::Value,
) {
    let handler_id = handler.id;
    let sender = if let Some((tx, _)) = lanes.get(&handler_id) {
        tx.clone()
    } else {
        let (tx, rx) = mpsc::channel::<LaneJob>(HANDLER_LANE_QUEUE_CAPACITY);
        let task = tokio::spawn(process_handler_lane(handler_id, rx));
        lanes.insert(handler_id, (tx.clone(), task));
        tx
    };

    match sender.try_send(LaneJob { handler, payload }) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => warning!(
            "sync: router: handler lane {} queue full; dropping event",
            handler_id
        ),
        Err(mpsc::error::TrySendError::Closed(job)) => {
            lanes.remove(&handler_id);
            let (tx, rx) =
                mpsc::channel::<LaneJob>(HANDLER_LANE_QUEUE_CAPACITY);
            let task = tokio::spawn(process_handler_lane(handler_id, rx));
            lanes.insert(handler_id, (tx.clone(), task));
            if tx.try_send(job).is_err() {
                warning!(
                    "sync: router: handler lane {} unavailable after restart; dropping event",
                    handler_id
                );
            }
        }
    }
}

async fn request_handlers(channel: &Channel) -> Option<Vec<HandlerRuntime>> {
    let (tx, rx) = oneshot::channel::<Vec<HandlerRuntime>>();
    if !channel.send(Message::Handlers(tx)) {
        return None;
    }
    rx.await.ok()
}

async fn listen_cron(channel: Arc<Channel>) {
    let mut next_runs: HashMap<i64, chrono::DateTime<chrono::Utc>> =
        HashMap::new();
    let mut cron_specs: HashMap<i64, String> = HashMap::new();
    let mut interval =
        tokio::time::interval(Duration::from_millis(CRON_POLL_MS));

    loop {
        interval.tick().await;

        let Some(handlers) = request_handlers(&channel).await else {
            warning!("sync: cron: failed to load handlers");
            continue;
        };

        let now = chrono::Utc::now();
        let mut active_ids = std::collections::HashSet::new();

        for handler in
            handlers.into_iter().filter(|h| h.options.is_cron_handler())
        {
            active_ids.insert(handler.id);
            let Some(cron_expr) = handler.options.cron.clone() else {
                warning!(
                    "sync: cron: handler {} missing runtime.cron",
                    handler.name
                );
                continue;
            };

            let schedule = match cron_expr.parse::<cron::Schedule>() {
                Ok(v) => v,
                Err(error) => {
                    warning!(
                        "sync: cron: handler {} invalid cron '{}': {}",
                        handler.name,
                        cron_expr,
                        error
                    );
                    continue;
                }
            };

            if cron_specs.get(&handler.id) != Some(&cron_expr) {
                cron_specs.insert(handler.id, cron_expr.clone());
                if let Some(next) = schedule.after(&now).next() {
                    next_runs.insert(handler.id, next);
                } else {
                    warning!(
                        "sync: cron: handler {} has no future schedule for '{}'",
                        handler.name,
                        cron_expr
                    );
                    next_runs.remove(&handler.id);
                }
                continue;
            }

            let Some(next) = next_runs.get(&handler.id).copied() else {
                if let Some(next) = schedule.after(&now).next() {
                    next_runs.insert(handler.id, next);
                }
                continue;
            };

            if now < next {
                continue;
            }

            if !channel.send(Message::CronTick(Arc::new(handler.clone()))) {
                warning!("sync: cron: enqueue failed");
                return;
            }

            if let Some(next_after) = schedule.after(&now).next() {
                next_runs.insert(handler.id, next_after);
            } else {
                next_runs.remove(&handler.id);
            }
        }

        next_runs.retain(|id, _| active_ids.contains(id));
        cron_specs.retain(|id, _| active_ids.contains(id));
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_sync(_arg: pg_sys::Datum) {
    // Auto-quit after n restarts, require manual restart
    if *RESTART_COUNT.exclusive() >= 5 {
        return;
    }

    *RESTART_COUNT.exclusive() += 1;

    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );

    if let Some(database) = DATABASE.get() {
        BackgroundWorker::connect_worker_to_spi(
            Some(database.to_str().expect("database name to be valid utf8")),
            None,
        );
    } else {
        error!("sync: database name was not provided");
    }

    if let Err(error) = anyhow_pg_try!(|| {
        let config_dir = config::resolve_config_dir()?;
        config::sync_from_handlers(&config_dir).map(|_| ())
    }) {
        warning!("sync: failed to sync handlers with {}", error);
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("sync: Failed to create async runtime");

    log!("sync: worker has started!");

    if let Err(error) = anyhow_pg_try!(|| {
        let handlers = HandlerRuntime::query_all()?;
        prepared::prepare_all_handlers(&handlers)
    }) {
        warning!("sync: failed to load handler queries: {:#}", error)
    }

    *WORKER_STATUS.exclusive() = WorkerStatus::RUNNING;

    let (send_message, receive_message) = mpsc::channel::<_>(MESSAGES_CAPACITY);

    let mut signal_bus = Bus::<Signal>::new(64);

    let channel = Arc::new(Channel::new(send_message));

    runtime.block_on(async {
        let evm_blocks_rx = signal_bus.add_rx();
        let evm_logs_rx = signal_bus.add_rx();

        let svm_blocks_rx = signal_bus.add_rx();
        let svm_logs_rx = signal_bus.add_rx();

        let handler =
            tokio::spawn(handle_message(MessageStream::new(receive_message)));

        tokio::select! {
             _ = worker::handle_signals(Arc::clone(&channel), signal_bus) => {
                 log!("sync: received exit signal... exiting");
             },
             _ = evm::blocks::listen(Arc::clone(&channel), evm_blocks_rx) => {
                 log!("sync: stopped listening to blocks... exiting");
             },
             _ = evm::logs::listen(Arc::clone(&channel), evm_logs_rx) => {
                 log!("sync: stopped listening to events... exiting");
             },
             _ = svm::blocks::listen(Arc::clone(&channel), svm_blocks_rx) => {
                 log!("sync: stopped listening to blocks... exiting");
             },
             _ = svm::logs::listen(Arc::clone(&channel), svm_logs_rx) => {
                 log!("sync: stopped listening to transactions... exiting");
             },
             _ = listen_cron(Arc::clone(&channel)) => {
                 log!("sync: stopped cron scheduler... exiting");
             },
        }

        if channel.send(Message::Shutdown) {
            if let Err(err) = handler.await {
                log!("sync: router: worker exited with error: {}", err);
            }
        }
    });

    *WORKER_STATUS.exclusive() = WorkerStatus::STOPPED;
    log!("sync: worker has exited");
}

async fn handle_message(mut stream: MessageStream) {
    let mut lanes: HashMap<i64, (mpsc::Sender<LaneJob>, JoinHandle<()>)> =
        HashMap::new();

    let evm_blocks = Arc::new(AtomicUsize::new(0));
    let evm_logs = Arc::new(AtomicUsize::new(0));
    let evm_blocks_stats = Arc::clone(&evm_blocks);
    let evm_logs_stats = Arc::clone(&evm_logs);

    let svm_blocks = Arc::new(AtomicUsize::new(0));
    let svm_logs = Arc::new(AtomicUsize::new(0));
    let cron_ticks = Arc::new(AtomicUsize::new(0));
    let svm_blocks_stats = Arc::clone(&svm_blocks);
    let svm_logs_stats = Arc::clone(&svm_logs);
    let cron_ticks_stats = Arc::clone(&cron_ticks);

    // Spawn stats logger
    let stats = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;
            log!(
                "sync: router: throughput evm(blocks/logs)={}/{} per min svm(blocks/logs)={}/{} per min cron(ticks)={} per min",
                evm_blocks_stats.swap(0, Ordering::Relaxed),
                evm_logs_stats.swap(0, Ordering::Relaxed),
                svm_blocks_stats.swap(0, Ordering::Relaxed),
                svm_logs_stats.swap(0, Ordering::Relaxed),
                cron_ticks_stats.swap(0, Ordering::Relaxed)
            );
        }
    });

    loop {
        let Some(message) = stream.next().await else {
            warning!("sync: router: message stream closed");
            break;
        };

        match message {
            Message::Handlers(oneshot) => match anyhow_pg_try!(|| {
                HandlerRuntime::query_all()
            }) {
                Ok(handlers) => {
                    if oneshot.send(handlers).is_err() {
                        warning!("sync: router: failed to return route table");
                    }
                }
                Err(error) => {
                    warning!(
                        "sync: router: failed to load route table: {}",
                        error
                    );
                }
            },
            Message::EvmBlock(block, handler) => {
                if handler.options.evm.is_none() {
                    error!(
                        "sync: router: evm:block route {} has non-evm config",
                        handler.name
                    );
                }

                evm_blocks.fetch_add(1, Ordering::Relaxed);

                let payload = json!({
                    "event": "evm_block",
                    "handler_id": handler.id,
                    "number": block.number,
                    "hash": format!("{:#x}", block.hash),
                });
                route_to_handler_lane(&mut lanes, handler, payload);
            }
            Message::EvmLog(log, handler) => {
                if handler.options.evm.is_none() {
                    error!(
                        "sync: router: evm:log route {} has non-evm config",
                        handler.name
                    );
                }

                evm_logs.fetch_add(1, Ordering::Relaxed);

                let topics = log
                    .topics()
                    .iter()
                    .map(|topic| format!("{:#x}", topic))
                    .collect::<Vec<_>>();
                let payload = json!({
                    "event": "evm_log",
                    "handler_id": handler.id,
                    "block_number": log.block_number,
                    "transaction_hash": log.transaction_hash.as_ref().map(|v| format!("{:#x}", v)),
                    "log_index": log.log_index,
                    "address": format!("{:#x}", log.address()),
                    "topics": topics,
                    "data": hex::encode(log.data().data.clone()),
                    "ingest_unix": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                });
                route_to_handler_lane(&mut lanes, handler, payload);
            }
            Message::SvmBlock(block, handler) => {
                if handler.options.svm.is_none() {
                    error!(
                        "sync: router: svm:block route {} has non-svm config",
                        handler.name
                    );
                }

                svm_blocks.fetch_add(1, Ordering::Relaxed);

                let payload = json!({
                    "event": "svm_block",
                    "handler_id": handler.id,
                    "block_height": block.block_height,
                    "block_hash": block.blockhash,
                });
                route_to_handler_lane(&mut lanes, handler, payload);
            }
            Message::SvmLog(log, handler) => {
                if handler.options.svm.is_none() {
                    error!(
                        "sync: router: svm:log route {} has non-svm config",
                        handler.name
                    );
                }

                svm_logs.fetch_add(1, Ordering::Relaxed);

                let payload = json!({
                    "event": "svm_log",
                    "handler_id": handler.id,
                    "slot": log.context.slot,
                    "signature": log.value.signature,
                    "logs": log.value.logs,
                });
                route_to_handler_lane(&mut lanes, handler, payload);
            }
            Message::CronTick(handler) => {
                if !handler.options.is_cron_handler() {
                    error!(
                        "sync: router: cron route {} has non-cron config",
                        handler.name
                    );
                }

                cron_ticks.fetch_add(1, Ordering::Relaxed);

                let payload = json!({
                    "event": "cron_tick",
                    "handler_id": handler.id,
                    "tick_unix": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                    "rpc": handler.options.rpc.clone(),
                    "ws": handler.options.ws.clone(),
                    "svm": handler.options.svm.clone(),
                    "evm": handler.options.evm.clone(),
                    "cron": handler.options.cron.clone(),
                });
                route_to_handler_lane(&mut lanes, handler, payload);
            }
            Message::Shutdown => {
                break;
            }
        }
    }

    stats.abort();
    for (_, (_, task)) in lanes {
        task.abort();
    }
}
