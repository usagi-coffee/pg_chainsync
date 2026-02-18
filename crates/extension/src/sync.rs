use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{OnceLock, RwLock};

use pgrx::bgworkers::*;
use pgrx::log;
use pgrx::prelude::*;

use tokio::sync::mpsc;
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

static PRELOOKUP_CACHE: OnceLock<RwLock<HashMap<i64, serde_json::Value>>> =
    OnceLock::new();

fn prelookup_cache() -> &'static RwLock<HashMap<i64, serde_json::Value>> {
    PRELOOKUP_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn resolve_prefetched(
    handler: &Arc<HandlerRuntime>,
) -> Result<Option<serde_json::Value>, anyhow::Error> {
    let handler_id = handler.id;
    let Some(prelookups) = &handler.options.prelookups else {
        return Ok(None);
    };
    if prelookups.is_empty() {
        return Ok(None);
    }

    if let Some(cached) = prelookup_cache()
        .read()
        .expect("prelookup cache read")
        .get(&handler.id)
        .cloned()
    {
        return Ok(Some(cached));
    }

    let mut values = serde_json::Map::new();
    for lookup_id in prelookups {
        let lookup_id_ref = lookup_id.as_str();
        let value =
            anyhow_pg_try!(|| prepared::execute_lookup(handler_id, lookup_id_ref))
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

    let prefetched = serde_json::Value::Object(values);
    prelookup_cache()
        .write()
        .expect("prelookup cache write")
        .insert(handler.id, prefetched.clone());
    Ok(Some(prefetched))
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
                let payload =
                    module_protocol::payload_to_jsonb(mutation.payload)?;
                anyhow_pg_try!(|| {
                    prepared::execute_mutation(handler_id, &mutation_id, payload)
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
    })
    {
        warning!("sync: failed to sync handlers with {}", error);
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("sync: Failed to create async runtime");

    log!("sync: worker has started!");
    prelookup_cache()
        .write()
        .expect("prelookup cache write")
        .clear();

    match anyhow_pg_try!(|| {
        let handlers = HandlerRuntime::query_all()?;
        prepared::prepare_all_handlers(&handlers)
    }) {
        Ok(total) => log!("sync: prepared {} handler queries", total),
        Err(error) => {
            warning!(
                "sync: failed to prepare handler queries: {:#}",
                error
            )
        }
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
    let evm_blocks = Arc::new(AtomicUsize::new(0));
    let evm_logs = Arc::new(AtomicUsize::new(0));
    let evm_blocks_stats = Arc::clone(&evm_blocks);
    let evm_logs_stats = Arc::clone(&evm_logs);

    let svm_blocks = Arc::new(AtomicUsize::new(0));
    let svm_logs = Arc::new(AtomicUsize::new(0));
    let svm_blocks_stats = Arc::clone(&svm_blocks);
    let svm_logs_stats = Arc::clone(&svm_logs);

    // Spawn stats logger
    let stats = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;
            log!(
                "sync: router: throughput evm(blocks/logs)={}/{} per min svm(blocks/logs)={}/{} per min",
                evm_blocks_stats.swap(0, Ordering::Relaxed),
                evm_logs_stats.swap(0, Ordering::Relaxed),
                svm_blocks_stats.swap(0, Ordering::Relaxed),
                svm_logs_stats.swap(0, Ordering::Relaxed)
            );
        }
    });

    loop {
        let Some(message) = stream.next().await else {
            warning!("sync: router: message stream closed");
            break;
        };

        match message {
            Message::Handlers(oneshot) => match anyhow_pg_try!(|| HandlerRuntime::query_all())
            {
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
                    error!("sync: router: evm:block route {} has non-evm config", handler.name);
                }

                evm_blocks.fetch_add(1, Ordering::Relaxed);

                let payload = json!({
                    "event": "evm_block",
                    "handler_id": handler.id,
                    "number": block.number,
                    "hash": format!("{:#x}", block.hash),
                });
                if let Err(error) = invoke_handler_module(&handler, payload) {
                    warning!(
                        "sync: router: evm:block route {} module invocation failed: {}",
                        handler.id,
                        error
                    );
                }
            }
            Message::EvmLog(log, handler) => {
                if handler.options.evm.is_none() {
                    error!("sync: router: evm:log route {} has non-evm config", handler.name);
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
                if let Err(error) = invoke_handler_module(&handler, payload) {
                    warning!(
                        "sync: router: evm:log route {} module invocation failed: {}",
                        handler.id,
                        error
                    );
                }
            }
            Message::SvmBlock(block, handler) => {
                if handler.options.svm.is_none() {
                    error!("sync: router: svm:block route {} has non-svm config", handler.name);
                }

                svm_blocks.fetch_add(1, Ordering::Relaxed);

                let payload = json!({
                    "event": "svm_block",
                    "handler_id": handler.id,
                    "block_height": block.block_height,
                    "block_hash": block.blockhash,
                });
                if let Err(error) = invoke_handler_module(&handler, payload) {
                    warning!(
                        "sync: router: svm:block route {} module invocation failed: {}",
                        handler.id,
                        error
                    );
                }
            }
            Message::SvmLog(log, handler) => {
                if handler.options.svm.is_none() {
                    error!("sync: router: svm:log route {} has non-svm config", handler.name);
                }

                svm_logs.fetch_add(1, Ordering::Relaxed);

                let payload = json!({
                    "event": "svm_log",
                    "handler_id": handler.id,
                    "slot": log.context.slot,
                    "signature": log.value.signature,
                    "logs": log.value.logs,
                });
                if let Err(error) = invoke_handler_module(&handler, payload) {
                    warning!(
                        "sync: router: svm:log route {} module invocation failed: {}",
                        handler.id,
                        error
                    );
                }
            }
            Message::Shutdown => {
                break;
            }
        }
    }

    stats.abort();
}
