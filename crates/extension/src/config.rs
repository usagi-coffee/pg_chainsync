use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use pgrx::{Spi, log};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::plugin;
use crate::types::{HandlerRuntime, HandlerOptions};
use crate::worker::CONFIG_DIR;

#[derive(Deserialize, Clone)]
struct HandlerHeader {
    id: String,
    chain: String,
    mode: String,
}

#[derive(Deserialize, Clone)]
struct QueryDefinition {
    sql: Option<String>,
    sql_inline: Option<String>,
}

#[derive(Deserialize, Clone)]
struct QuerySection {
    lookups: Option<BTreeMap<String, QueryDefinition>>,
    mutations: Option<BTreeMap<String, QueryDefinition>>,
}

#[derive(Deserialize, Clone)]
struct RuntimeSection {
    prelookups: Option<Vec<String>>,
}

#[derive(Deserialize, Clone)]
struct HandlerToml {
    handler: HandlerHeader,
    rpc: Option<String>,
    ws: Option<String>,
    queries: Option<QuerySection>,
    runtime: Option<RuntimeSection>,
    evm: Option<crate::types::EvmOptions>,
    svm: Option<crate::types::SvmOptions>,
}

#[derive(Serialize)]
pub struct RuntimeStatus {
    pub handler_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_name: Option<String>,
    pub status: String,
    pub last_error: Option<String>,
}

pub struct SyncOutcome {
    pub statuses: Vec<RuntimeStatus>,
    pub restart_blocks: bool,
    pub restart_logs: bool,
}

struct LoadedHandler {
    id: String,
    module_name: String,
    content_hash: String,
    options: HandlerOptions,
}

fn stable_handler_id(name: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    (hasher.finish() & 0x7fff_ffff_ffff_ffff) as i64
}

fn resolve_env(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut index = 0usize;

    while let Some(start_rel) = input[index..].find("${") {
        let start = index + start_rel;
        out.push_str(&input[index..start]);
        let Some(end_rel) = input[start + 2..].find('}') else {
            bail!("unterminated env placeholder in value");
        };
        let end = start + 2 + end_rel;
        let key = &input[start + 2..end];
        let value = env::var(key)
            .with_context(|| format!("missing environment variable {}", key))?;
        out.push_str(&value);
        index = end + 1;
    }

    out.push_str(&input[index..]);
    Ok(out)
}

fn resolve_opt(value: Option<String>) -> Result<Option<String>> {
    value.map(|v| resolve_env(&v)).transpose()
}

fn discover_handler_modules(config_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut modules = Vec::new();
    for entry in fs::read_dir(config_dir)
        .with_context(|| format!("reading dir {}", config_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("so") {
            modules.push(path);
        }
    }

    modules.sort();
    Ok(modules)
}

fn materialize_query_sql(
    queries: &QuerySection,
) -> Result<(
    Option<BTreeMap<String, String>>,
    Option<BTreeMap<String, String>>,
)> {
    let mut lookups: Option<BTreeMap<String, String>> = None;
    let mut mutations: Option<BTreeMap<String, String>> = None;

    if let Some(lookup_defs) = &queries.lookups {
        let mut map = BTreeMap::new();
        for (id, def) in lookup_defs {
            let has_path = def.sql.is_some();
            let has_inline = def.sql_inline.is_some();
            if has_path == has_inline {
                bail!(
                    "lookup query '{}' must define exactly one of sql or sql_inline",
                    id
                );
            }
            if let Some(path) = &def.sql {
                bail!(
                    "lookup query '{}' uses sql='{}', but SO-only mode requires sql_inline",
                    id,
                    path
                );
            } else if let Some(sql_inline) = &def.sql_inline {
                map.insert(id.clone(), sql_inline.clone());
            }
        }
        lookups = Some(map);
    }

    if let Some(mutation_defs) = &queries.mutations {
        let mut map = BTreeMap::new();
        for (id, def) in mutation_defs {
            let has_path = def.sql.is_some();
            let has_inline = def.sql_inline.is_some();
            if has_path == has_inline {
                bail!(
                    "mutation query '{}' must define exactly one of sql or sql_inline",
                    id
                );
            }
            if let Some(path) = &def.sql {
                bail!(
                    "mutation query '{}' uses sql='{}', but SO-only mode requires sql_inline",
                    id,
                    path
                );
            } else if let Some(sql_inline) = &def.sql_inline {
                map.insert(id.clone(), sql_inline.clone());
            }
        }
        mutations = Some(map);
    }

    Ok((lookups, mutations))
}

fn validate_prelookups(
    prelookups: &Option<Vec<String>>,
    lookup_queries: &Option<BTreeMap<String, String>>,
) -> Result<()> {
    let Some(prelookups) = prelookups else {
        return Ok(());
    };
    let Some(lookup_queries) = lookup_queries else {
        bail!("runtime.prelookups declared but no queries.lookups are defined");
    };

    for prelookup in prelookups {
        if !lookup_queries.contains_key(prelookup) {
            bail!(
                "runtime.prelookups references undefined lookup query '{}'",
                prelookup
            );
        }
    }

    Ok(())
}

fn hash_handler_module(handler_module: &Path) -> Result<String> {
    let mut hasher = DefaultHasher::new();
    let bytes = fs::read(handler_module)
        .with_context(|| format!("reading {}", handler_module.display()))?;
    handler_module.to_string_lossy().hash(&mut hasher);
    bytes.hash(&mut hasher);
    Ok(format!("{:016x}", hasher.finish()))
}

fn parse_handler(config_dir: &Path, handler_module: &Path) -> Result<LoadedHandler> {
    let source = plugin::read_handler_toml(handler_module).with_context(|| {
        format!("reading embedded handler.toml from {}", handler_module.display())
    })?;
    let parsed: HandlerToml = toml::from_str(&source).with_context(|| {
        format!("parsing embedded handler.toml from {}", handler_module.display())
    })?;

    if parsed.handler.id.trim().is_empty() {
        bail!("handler.id is empty");
    }

    if parsed.handler.chain != "evm" && parsed.handler.chain != "svm" {
        bail!("handler.chain must be one of: evm, svm");
    }
    if parsed.handler.chain == "evm" && parsed.evm.is_none() {
        bail!("handler.chain=evm requires [evm] section");
    }
    if parsed.handler.chain == "svm" && parsed.svm.is_none() {
        bail!("handler.chain=svm requires [svm] section");
    }

    if parsed.handler.mode != "stream" {
        bail!("handler.mode must be 'stream' in v2");
    }

    if !handler_module.is_file() {
        bail!("missing module binary {}", handler_module.display());
    }

    let metadata = plugin::validate_plugin(handler_module)
        .with_context(|| format!("invalid module {}", handler_module.display()))?;
    plugin::validate_handler_exports(handler_module).with_context(|| {
        format!("invalid handler exports {}", handler_module.display())
    })?;
    let _ = (
        metadata.abi_major,
        metadata.abi_minor,
        metadata.name,
        metadata.version,
    );

    let (lookup_queries, mutation_queries) = match parsed.queries {
        Some(ref queries) => materialize_query_sql(queries)?,
        None => (None, None),
    };

    let prelookups = parsed.runtime.and_then(|runtime| runtime.prelookups);
    validate_prelookups(&prelookups, &lookup_queries)?;
    let content_hash = hash_handler_module(handler_module)?;

    let module_name = handler_module
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    Ok(LoadedHandler {
        id: parsed.handler.id.clone(),
        module_name,
        content_hash: content_hash.clone(),
        options: HandlerOptions {
            rpc: resolve_opt(parsed.rpc)?,
            ws: resolve_opt(parsed.ws)?,
            lookup_queries,
            mutation_queries,
            prelookups,
            module_path: Some(handler_module.to_string_lossy().into_owned()),
            content_hash: Some(content_hash),
            state_path: Some(
                config_dir
                    .parent()
                    .unwrap_or(config_dir)
                    .join("state")
                    .join(format!("{}.bin", &parsed.handler.id))
                    .to_string_lossy()
                    .into_owned(),
            ),
            evm: parsed.evm,
            svm: parsed.svm,
        },
    })
}

fn write_handler_status(
    config_dir: &Path,
    status: &RuntimeStatus,
) -> Result<()> {
    let chainsync_dir = config_dir.parent().unwrap_or(config_dir);
    let status_dir = chainsync_dir.join("status");
    fs::create_dir_all(&status_dir).with_context(|| {
        format!(
            "creating status directory {}",
            status_dir.display()
        )
    })?;

    let status_path = status_dir.join(format!("{}.json", status.handler_id));
    let status_json = serde_json::to_string_pretty(status)?;
    fs::write(&status_path, status_json)
        .with_context(|| format!("writing {}", status_path.display()))?;

    let logs_dir = chainsync_dir.join("logs");
    fs::create_dir_all(&logs_dir)
        .with_context(|| format!("creating logs directory {}", logs_dir.display()))?;
    let state_dir = chainsync_dir.join("state");
    fs::create_dir_all(&state_dir)
        .with_context(|| format!("creating state directory {}", state_dir.display()))?;
    let log_name = status
        .module_name
        .as_deref()
        .unwrap_or(status.handler_id.as_str());
    let log_path = logs_dir.join(format!("{}.log", log_name));
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("opening {}", log_path.display()))?;
    let line = serde_json::json!({
        "status": status.status,
        "last_error": status.last_error,
        "timestamp_unix": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    });
    writeln!(file, "{}", line)
        .with_context(|| format!("writing {}", log_path.display()))?;

    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct RoutingSnapshot {
    lane: &'static str,
    key: String,
}

fn routing_snapshots(handlers: &[HandlerRuntime]) -> Vec<RoutingSnapshot> {
    let mut snapshots = Vec::new();
    for handler in handlers {
        if let Some(evm) = &handler.options.evm {
            let ws = handler.options.ws.as_deref().unwrap_or("<missing-ws>");
            if handler.options.is_block_handler() {
                snapshots.push(RoutingSnapshot {
                    lane: "blocks",
                    key: format!("evm|ws={}", ws),
                });
            }
            if handler.options.is_log_handler() {
                snapshots.push(RoutingSnapshot {
                    lane: "logs",
                    key: format!(
                        "evm|ws={}|address={:?}|event={:?}|t0={:?}|t1={:?}|t2={:?}|t3={:?}|from={:?}|to={:?}",
                        ws,
                        evm.address,
                        evm.event,
                        evm.topic0,
                        evm.topic1,
                        evm.topic2,
                        evm.topic3,
                        evm.from_block,
                        evm.to_block
                    ),
                });
            }
        }

        if let Some(svm) = &handler.options.svm {
            let rpc = handler.options.rpc.as_deref().unwrap_or("<missing-rpc>");
            if handler.options.is_block_handler() {
                snapshots.push(RoutingSnapshot {
                    lane: "blocks",
                    key: format!(
                        "svm|rpc={}|mentions={:?}|tx_details={:?}",
                        rpc, svm.mentions, svm.transaction_details
                    ),
                });
            }
            if handler.options.is_log_handler() {
                snapshots.push(RoutingSnapshot {
                    lane: "logs",
                    key: format!("svm|rpc={}|mentions={:?}", rpc, svm.mentions),
                });
            }
        }
    }
    snapshots.sort();
    snapshots
}

pub fn resolve_config_dir() -> Result<PathBuf> {
    if let Some(config_dir) = CONFIG_DIR.get()
        && let Ok(config_dir) = config_dir.to_str()
        && !config_dir.trim().is_empty()
    {
        return Ok(PathBuf::from(config_dir));
    }

    let data_dir = Spi::get_one::<String>("SHOW data_directory")?
        .context("SHOW data_directory returned no value")?;
    Ok(PathBuf::from(data_dir).join("chainsync").join("handlers"))
}

pub fn sync_from_handlers(config_dir: &Path) -> Result<SyncOutcome> {
    fs::create_dir_all(config_dir).with_context(|| {
        format!("creating config directory {}", config_dir.display())
    })?;
    let handler_modules = discover_handler_modules(config_dir)?;
    let mut seen = HashSet::new();
    let mut statuses = Vec::new();
    let mut loaded_handlers = Vec::new();
    let previous_handlers = HandlerRuntime::query_all().unwrap_or_default();
    let previous_handler_names = previous_handlers
        .iter()
        .map(|handler| handler.name.clone())
        .collect::<HashSet<_>>();
    let previous_hashes = previous_handlers
        .iter()
        .map(|handler| {
            (
                handler.name.clone(),
                handler.options.content_hash.clone().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let previous_snapshots = routing_snapshots(&previous_handlers);
    for handler_module in handler_modules {
        let result = parse_handler(config_dir, &handler_module).and_then(|handler| {
            if !seen.insert(handler.id.clone()) {
                bail!("duplicate handler.id {}", handler.id);
            }
            let was_known = previous_handler_names.contains(&handler.id);
            let previous_hash = previous_hashes.get(&handler.id);
            let changed =
                !matches!(previous_hash, Some(existing) if existing == &handler.content_hash);

            loaded_handlers.push(HandlerRuntime {
                id: stable_handler_id(&handler.id),
                name: handler.id.clone(),
                status: "STOPPED".to_string(),
                options: handler.options,
                evm: OnceCell::const_new(),
                svm_ws: OnceCell::const_new(),
                svm_rpc: OnceCell::const_new(),
            });

            if !was_known {
                log!("sync: handlers: registered {}", handler.id);
                let status = RuntimeStatus {
                    handler_id: handler.id.clone(),
                    module_name: Some(handler.module_name.clone()),
                    status: "REGISTERED".into(),
                    last_error: None,
                };
                write_handler_status(config_dir, &status)?;
                statuses.push(status);
            } else if changed {
                log!("sync: handlers: updated {}", handler.id);
                let status = RuntimeStatus {
                    handler_id: handler.id.clone(),
                    module_name: Some(handler.module_name.clone()),
                    status: "UPDATED".into(),
                    last_error: None,
                };
                write_handler_status(config_dir, &status)?;
                statuses.push(status);
            }

            Ok(handler.id)
        });

        if let Err(error) = result {
            let fallback_id = handler_module
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();
            let status = RuntimeStatus {
                handler_id: fallback_id,
                module_name: handler_module
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string()),
                status: "ERROR".into(),
                last_error: Some(error.to_string()),
            };
            write_handler_status(config_dir, &status)?;
            statuses.push(status);
        }
    }

    let stale_ids: Vec<String> = previous_handler_names
        .into_iter()
        .filter(|name| !seen.contains(name))
        .collect();

    for stale in stale_ids {
        log!("sync: handlers: deregistered {}", stale);
        let status = RuntimeStatus {
            handler_id: stale,
            module_name: None,
            status: "REMOVED".into(),
            last_error: None,
        };
        write_handler_status(config_dir, &status)?;
        statuses.push(status);
    }

    let next_snapshots = routing_snapshots(&loaded_handlers);
    let previous_blocks = previous_snapshots
        .iter()
        .filter(|snapshot| snapshot.lane == "blocks")
        .cloned()
        .collect::<Vec<_>>();
    let next_blocks = next_snapshots
        .iter()
        .filter(|snapshot| snapshot.lane == "blocks")
        .cloned()
        .collect::<Vec<_>>();
    let previous_logs = previous_snapshots
        .iter()
        .filter(|snapshot| snapshot.lane == "logs")
        .cloned()
        .collect::<Vec<_>>();
    let next_logs = next_snapshots
        .iter()
        .filter(|snapshot| snapshot.lane == "logs")
        .cloned()
        .collect::<Vec<_>>();
    let restart_blocks = previous_blocks != next_blocks;
    let restart_logs = previous_logs != next_logs;

    HandlerRuntime::replace_all(loaded_handlers);

    Ok(SyncOutcome {
        statuses,
        restart_blocks,
        restart_logs,
    })
}
