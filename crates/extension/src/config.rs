use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use pgrx::{JsonB, Spi, log, warning};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::module_runtime;
use crate::plugin;
use crate::types::{HandlerMode, HandlerOptions, HandlerRuntime};

#[derive(Deserialize, Clone)]
struct HandlerHeader {
    id: String,
    plugin: String,
    chain: String,
    mode: String,
}

#[derive(Deserialize, Clone)]
struct RuntimeSection {
    enrich: Option<Vec<String>>,
    cron: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct IngressSection {
    rpc: Option<String>,
    ws: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct HandlerToml {
    handler: HandlerHeader,
    plugin: Option<toml::value::Table>,
    ingress: Option<IngressSection>,
    queries: Option<toml::value::Table>,
    runtime: Option<RuntimeSection>,
    evm: Option<crate::types::EvmOptions>,
    svm: Option<crate::types::SvmOptions>,
}

#[derive(Debug, Deserialize)]
struct SetupResponse {
    ingress_overrides: Option<IngressOverrides>,
}

#[derive(Debug, Deserialize)]
struct IngressOverrides {
    ws: Option<String>,
    rpc: Option<String>,
    evm: Option<EvmIngressOverrides>,
    svm: Option<SvmIngressOverrides>,
}

#[derive(Debug, Deserialize)]
struct EvmIngressOverrides {
    from_block: Option<i64>,
    to_block: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SvmIngressOverrides {
    from_slot: Option<u64>,
    to_slot: Option<u64>,
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

struct LoadedPlugin {
    path: PathBuf,
    content_hash: String,
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

fn resolve_env_in_toml(value: &mut toml::Value) -> Result<()> {
    match value {
        toml::Value::String(raw) => {
            *raw = resolve_env(raw)?;
        }
        toml::Value::Array(items) => {
            for item in items {
                resolve_env_in_toml(item)?;
            }
        }
        toml::Value::Table(table) => {
            for (_, value) in table.iter_mut() {
                resolve_env_in_toml(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<String> {
    let mut hasher = DefaultHasher::new();
    let bytes = fs::read(path)
        .with_context(|| format!("reading {}", path.display()))?;
    path.to_string_lossy().hash(&mut hasher);
    bytes.hash(&mut hasher);
    Ok(format!("{:016x}", hasher.finish()))
}

fn discover_plugins(
    chainsync_dir: &Path,
) -> Result<HashMap<String, LoadedPlugin>> {
    let plugins_dir = chainsync_dir.join("plugins");
    fs::create_dir_all(&plugins_dir)
        .with_context(|| format!("creating {}", plugins_dir.display()))?;

    let mut plugins = HashMap::new();
    for entry in fs::read_dir(&plugins_dir)
        .with_context(|| format!("reading dir {}", plugins_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("so") {
            continue;
        }

        plugin::validate_plugin(&path)
            .with_context(|| format!("invalid plugin {}", path.display()))?;
        plugin::validate_handler_exports(&path).with_context(|| {
            format!("invalid plugin exports {}", path.display())
        })?;

        let plugin_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .context("plugin filename is not valid utf8")?
            .to_string();

        if plugins.contains_key(&plugin_name) {
            bail!("duplicate plugin '{}'", plugin_name);
        }

        plugins.insert(
            plugin_name,
            LoadedPlugin {
                path: path.clone(),
                content_hash: hash_file(&path)?,
            },
        );
    }

    if plugins.is_empty() {
        warning!(
            "sync: plugins: no plugin modules found in {}",
            plugins_dir.display()
        );
    }

    Ok(plugins)
}

fn discover_handler_files(chainsync_dir: &Path) -> Result<Vec<PathBuf>> {
    let handlers_dir = chainsync_dir.join("handlers");
    fs::create_dir_all(&handlers_dir)
        .with_context(|| format!("creating {}", handlers_dir.display()))?;

    let mut handler_files = Vec::new();
    for entry in fs::read_dir(&handlers_dir)
        .with_context(|| format!("reading dir {}", handlers_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) == Some("toml") {
            handler_files.push(path);
        }
    }

    handler_files.sort();
    Ok(handler_files)
}

fn materialize_query_sql(
    queries: &toml::value::Table,
) -> Result<Option<BTreeMap<String, String>>> {
    let mut merged = BTreeMap::new();

    for (id, value) in queries {
        let toml::Value::String(sql) = value else {
            bail!(
                "queries.{} must be an inline SQL string",
                id
            );
        };
        merged.insert(id.clone(), sql.clone());
    }

    let queries = if merged.is_empty() {
        None
    } else {
        Some(merged)
    };

    Ok(queries)
}

fn validate_enrich(
    enrich: &Option<Vec<String>>,
    queries: &Option<BTreeMap<String, String>>,
) -> Result<()> {
    let Some(enrich) = enrich else {
        return Ok(());
    };
    let Some(queries) = queries else {
        bail!("runtime.enrich declared but no queries are defined");
    };

    for enrich in enrich {
        if !queries.contains_key(enrich) {
            bail!("runtime.enrich references undefined query '{}'", enrich);
        }
    }

    Ok(())
}

fn validate_cron(mode: &HandlerMode, cron: &Option<String>) -> Result<()> {
    if !matches!(mode, HandlerMode::Cron) {
        return Ok(());
    }
    let Some(expr) = cron else {
        bail!("handler.mode=cron requires runtime.cron");
    };
    let _ = expr
        .parse::<cron::Schedule>()
        .with_context(|| format!("invalid runtime.cron '{}'", expr))?;
    Ok(())
}

fn parse_handler(
    chainsync_dir: &Path,
    handler_file: &Path,
    plugins: &HashMap<String, LoadedPlugin>,
) -> Result<LoadedHandler> {
    let source = fs::read_to_string(handler_file)
        .with_context(|| format!("reading {}", handler_file.display()))?;
    let parsed: HandlerToml = toml::from_str(&source)
        .with_context(|| format!("parsing {}", handler_file.display()))?;

    let ingress_rpc = parsed.ingress.as_ref().and_then(|i| i.rpc.clone());
    let ingress_ws = parsed.ingress.as_ref().and_then(|i| i.ws.clone());

    let effective_rpc = ingress_rpc;
    let effective_ws = ingress_ws;
    let effective_evm = parsed.evm.clone();
    let effective_svm = parsed.svm.clone();

    if parsed.handler.id.trim().is_empty() {
        bail!("handler.id is empty");
    }
    if parsed.handler.plugin.trim().is_empty() {
        bail!("handler.plugin is empty");
    }
    if parsed.handler.chain != "evm" && parsed.handler.chain != "svm" {
        bail!("handler.chain must be one of: evm, svm");
    }
    if parsed.handler.chain == "evm" && effective_evm.is_none() {
        bail!("handler.chain=evm requires [evm] section");
    }
    if parsed.handler.chain == "svm" && effective_svm.is_none() {
        bail!("handler.chain=svm requires [svm] section");
    }
    let mode = HandlerMode::try_from(parsed.handler.mode.as_str())
        .map_err(anyhow::Error::msg)?;

    if matches!(mode, HandlerMode::Stream)
        && parsed.handler.chain == "evm"
        && effective_ws.as_deref().is_none_or(|v| v.trim().is_empty())
    {
        bail!("handler.chain=evm requires ingress.ws in stream mode");
    }

    let plugin = plugins.get(&parsed.handler.plugin).with_context(|| {
        format!(
            "handler.plugin '{}' not found in {}/plugins",
            parsed.handler.plugin,
            chainsync_dir.display()
        )
    })?;

    let queries = match parsed.queries {
        Some(ref queries) => materialize_query_sql(queries)?,
        None => None,
    };

    let enrich = parsed
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.enrich.clone());
    let cron = parsed
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.cron.clone());
    validate_enrich(&enrich, &queries)?;
    validate_cron(&mode, &cron)?;

    let plugin_config = if let Some(table) = parsed.plugin {
        let mut value = toml::Value::Table(table);
        resolve_env_in_toml(&mut value)?;
        Some(serde_json::to_value(value)?)
    } else {
        None
    };

    let mut content_hasher = DefaultHasher::new();
    source.hash(&mut content_hasher);
    plugin.content_hash.hash(&mut content_hasher);
    let content_hash = format!("{:016x}", content_hasher.finish());

    let handler_id = parsed.handler.id.clone();
    Ok(LoadedHandler {
        id: handler_id.clone(),
        module_name: handler_id.clone(),
        content_hash: content_hash.clone(),
        options: HandlerOptions {
            mode,
            rpc: resolve_opt(effective_rpc)?,
            ws: resolve_opt(effective_ws)?,
            queries,
            enrich,
            cron,
            plugin: plugin_config,
            plugin_path: Some(plugin.path.to_string_lossy().into_owned()),
            content_hash: Some(content_hash),
            state_path: Some(
                chainsync_dir
                    .join("state")
                    .join(format!("{}.bin", handler_id))
                    .to_string_lossy()
                    .into_owned(),
            ),
            log_path: Some(
                chainsync_dir
                    .join("logs")
                    .join(format!("{}.log", handler_id))
                    .to_string_lossy()
                    .into_owned(),
            ),
            evm: effective_evm,
            svm: effective_svm,
        },
    })
}

fn write_handler_status(
    chainsync_dir: &Path,
    status: &RuntimeStatus,
) -> Result<()> {
    let logs_dir = chainsync_dir.join("logs");
    fs::create_dir_all(&logs_dir).with_context(|| {
        format!("creating logs directory {}", logs_dir.display())
    })?;
    let state_dir = chainsync_dir.join("state");
    fs::create_dir_all(&state_dir).with_context(|| {
        format!("creating state directory {}", state_dir.display())
    })?;

    let log_path = logs_dir.join(format!("{}.log", status.handler_id));
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

fn run_handler_setup(
    handler: &HandlerRuntime,
) -> Result<Option<IngressOverrides>> {
    let prefetched = setup_prefetched(&handler.options)?;
    let payload = serde_json::json!({
        "handler_id": handler.id,
        "handler_name": handler.name,
        "state_path": handler.options.state_path,
        "log_path": handler.options.log_path,
        "prefetched": prefetched,
        "plugin": handler.options.plugin.clone(),
    });
    let bytes = serde_json::to_vec(&payload)?;
    let response = module_runtime::setup(handler, &bytes)?;
    if response.is_null() {
        return Ok(None);
    }
    let parsed: SetupResponse = serde_json::from_value(response)
        .context("invalid setup response shape")?;
    Ok(parsed.ingress_overrides)
}

fn setup_prefetched(
    options: &HandlerOptions,
) -> Result<Option<serde_json::Value>> {
    let Some(enrich) = &options.enrich else {
        return Ok(None);
    };
    if enrich.is_empty() {
        return Ok(None);
    }
    let Some(queries) = &options.queries else {
        return Ok(None);
    };

    let mut values = serde_json::Map::new();
    for query_id in enrich {
        let sql = queries.get(query_id).with_context(|| {
            format!("setup enrich '{}' not found in queries", query_id)
        })?;
        let normalized = sql.trim().trim_end_matches(';');
        let wrapped = format!(
            "SELECT COALESCE(jsonb_agg(t), '[]'::jsonb) FROM ({}) AS t",
            normalized
        );
        let result = Spi::get_one::<JsonB>(&wrapped)
            .with_context(|| format!("executing setup enrich '{}'", query_id))?
            .context(format!(
                "setup enrich '{}' did not return jsonb",
                query_id
            ))?;
        values.insert(query_id.clone(), result.0);
    }

    Ok(Some(serde_json::Value::Object(values)))
}

fn apply_ingress_overrides(
    options: &mut HandlerOptions,
    overrides: IngressOverrides,
) {
    if let Some(ws) = overrides.ws {
        options.ws = Some(ws);
    }
    if let Some(rpc) = overrides.rpc {
        options.rpc = Some(rpc);
    }
    if let Some(evm_overrides) = overrides.evm
        && let Some(evm) = options.evm.as_mut()
    {
        if evm_overrides.from_block.is_some() {
            evm.from_block = evm_overrides.from_block;
        }
        if evm_overrides.to_block.is_some() {
            evm.to_block = evm_overrides.to_block;
        }
    }
    if let Some(svm_overrides) = overrides.svm
        && let Some(svm) = options.svm.as_mut()
    {
        if svm_overrides.from_slot.is_some() {
            svm.from_slot = svm_overrides.from_slot;
        }
        if svm_overrides.to_slot.is_some() {
            svm.to_slot = svm_overrides.to_slot;
        }
    }
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
    let data_dir = Spi::get_one::<String>("SHOW data_directory")?
        .context("SHOW data_directory returned no value")?;
    Ok(PathBuf::from(data_dir).join("chainsync"))
}

pub fn sync_from_handlers(chainsync_dir: &Path) -> Result<SyncOutcome> {
    fs::create_dir_all(chainsync_dir)
        .with_context(|| format!("creating {}", chainsync_dir.display()))?;
    let plugins = discover_plugins(chainsync_dir)?;
    let handler_files = discover_handler_files(chainsync_dir)?;

    if handler_files.is_empty() {
        warning!(
            "sync: handlers: no handler TOML files found in {}",
            chainsync_dir.join("handlers").display()
        );
    }

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

    for handler_file in handler_files {
        let result = parse_handler(chainsync_dir, &handler_file, &plugins)
            .and_then(|handler| {
                if !seen.insert(handler.id.clone()) {
                    bail!("duplicate handler.id {}", handler.id);
                }
                let was_known = previous_handler_names.contains(&handler.id);
                let previous_hash = previous_hashes.get(&handler.id);
                let changed = !matches!(
                    previous_hash,
                    Some(existing) if existing == &handler.content_hash
                );

                let mut runtime = HandlerRuntime {
                    id: stable_handler_id(&handler.id),
                    name: handler.id.clone(),
                    status: "STOPPED".to_string(),
                    options: handler.options,
                    evm: OnceCell::const_new(),
                    svm_ws: OnceCell::const_new(),
                    svm_rpc: OnceCell::const_new(),
                };
                if let Some(overrides) = run_handler_setup(&runtime)
                    .with_context(|| {
                        format!("running setup for handler {}", runtime.name)
                    })?
                {
                    apply_ingress_overrides(&mut runtime.options, overrides);
                }
                loaded_handlers.push(runtime);

                if !was_known {
                    log!("sync: handlers: registered {}", handler.id);
                    let status = RuntimeStatus {
                        handler_id: handler.id.clone(),
                        module_name: Some(handler.module_name.clone()),
                        status: "REGISTERED".into(),
                        last_error: None,
                    };
                    write_handler_status(chainsync_dir, &status)?;
                    statuses.push(status);
                } else if changed {
                    log!("sync: handlers: updated {}", handler.id);
                    let status = RuntimeStatus {
                        handler_id: handler.id.clone(),
                        module_name: Some(handler.module_name.clone()),
                        status: "UPDATED".into(),
                        last_error: None,
                    };
                    write_handler_status(chainsync_dir, &status)?;
                    statuses.push(status);
                }

                Ok(handler.id)
            });

        if let Err(error) = result {
            warning!(
                "sync: handlers: failed to load {}: {}",
                handler_file.display(),
                error
            );
            let fallback_id = handler_file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();
            let status = RuntimeStatus {
                handler_id: fallback_id,
                module_name: None,
                status: "ERROR".into(),
                last_error: Some(error.to_string()),
            };
            write_handler_status(chainsync_dir, &status)?;
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
        write_handler_status(chainsync_dir, &status)?;
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
