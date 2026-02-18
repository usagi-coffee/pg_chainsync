use std::collections::BTreeMap;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use serde::{Deserialize, Serialize};

use crate::plugin;
use crate::types::JobOptions;

#[derive(Deserialize, Clone)]
struct HandlerHeader {
    id: String,
    chain: String,
    mode: String,
}

#[derive(Deserialize, Clone)]
struct QueryDefinition {
    sql: String,
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
    preload: Option<bool>,
    oneshot: Option<bool>,
    cron: Option<String>,
    setup_handler: Option<String>,
    success_handler: Option<String>,
    failure_handler: Option<String>,
    queries: Option<QuerySection>,
    runtime: Option<RuntimeSection>,
    evm: Option<crate::types::EvmOptions>,
    svm: Option<crate::types::SvmOptions>,
}

#[derive(Serialize)]
pub struct RuntimeStatus {
    pub job_id: String,
    pub status: String,
    pub last_error: Option<String>,
}

struct LoadedHandler {
    id: String,
    options: JobOptions,
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

fn discover_handler_dirs(config_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for entry in fs::read_dir(config_dir)
        .with_context(|| format!("reading dir {}", config_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|name| name == "_runtime")
        {
            continue;
        }

        let handler_toml = path.join("handler.toml");
        if handler_toml.is_file() {
            dirs.push(path);
        }
    }

    dirs.sort();
    Ok(dirs)
}

fn validate_query_paths(
    handler_dir: &Path,
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
            let path = handler_dir.join(&def.sql);
            if !path.is_file() {
                bail!(
                    "lookup query '{}' points to missing file {}",
                    id,
                    path.display()
                );
            }
            map.insert(id.clone(), def.sql.clone());
        }
        lookups = Some(map);
    }

    if let Some(mutation_defs) = &queries.mutations {
        let mut map = BTreeMap::new();
        for (id, def) in mutation_defs {
            let path = handler_dir.join(&def.sql);
            if !path.is_file() {
                bail!(
                    "mutation query '{}' points to missing file {}",
                    id,
                    path.display()
                );
            }
            map.insert(id.clone(), def.sql.clone());
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

fn parse_handler(handler_dir: &Path) -> Result<LoadedHandler> {
    let handler_toml_path = handler_dir.join("handler.toml");
    let source = fs::read_to_string(&handler_toml_path)
        .with_context(|| format!("reading {}", handler_toml_path.display()))?;
    let parsed: HandlerToml = toml::from_str(&source).with_context(|| {
        format!("parsing TOML {}", handler_toml_path.display())
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

    let oneshot = match parsed.handler.mode.as_str() {
        "oneshot" => Some(true),
        "stream" => Some(false),
        "cron" => Some(false),
        _ => bail!("handler.mode must be one of: stream, oneshot, cron"),
    };

    if parsed.handler.mode == "cron" && parsed.cron.is_none() {
        bail!("cron mode requires top-level cron value");
    }

    let handler_so = handler_dir.join("handler.so");
    if !handler_so.is_file() {
        bail!("missing module binary {}", handler_so.display());
    }

    let metadata = plugin::validate_plugin(&handler_so)
        .with_context(|| format!("invalid module {}", handler_so.display()))?;
    plugin::validate_handler_exports(&handler_so).with_context(|| {
        format!("invalid handler exports {}", handler_so.display())
    })?;
    let _ = (
        metadata.abi_major,
        metadata.abi_minor,
        metadata.name,
        metadata.version,
    );

    let (lookup_queries, mutation_queries) = match parsed.queries {
        Some(ref queries) => validate_query_paths(handler_dir, queries)?,
        None => (None, None),
    };

    let prelookups = parsed.runtime.and_then(|runtime| runtime.prelookups);
    validate_prelookups(&prelookups, &lookup_queries)?;

    let setup_handler = parsed.setup_handler.map(Into::into);
    let success_handler = parsed.success_handler.map(Into::into);
    let failure_handler = parsed.failure_handler.map(Into::into);

    Ok(LoadedHandler {
        id: parsed.handler.id.clone(),
        options: JobOptions {
            rpc: resolve_opt(parsed.rpc)?,
            ws: resolve_opt(parsed.ws)?,
            preload: parsed.preload,
            oneshot: parsed.oneshot.or(oneshot),
            cron: resolve_opt(parsed.cron)?,
            setup_handler,
            success_handler,
            failure_handler,
            lookup_queries,
            mutation_queries,
            prelookups,
            handler_dir: Some(handler_dir.to_string_lossy().into_owned()),
            state_path: Some(
                handler_dir
                    .parent()
                    .unwrap_or(handler_dir)
                    .join("_runtime")
                    .join(&parsed.handler.id)
                    .join("state.bin")
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
    let handler_dir = config_dir.join("_runtime").join(&status.job_id);
    let logs_dir = handler_dir.join("logs");
    fs::create_dir_all(&logs_dir).with_context(|| {
        format!(
            "creating handler runtime directory {}",
            handler_dir.display()
        )
    })?;

    let status_path = handler_dir.join("status.json");
    let status_json = serde_json::to_string_pretty(status)?;
    fs::write(&status_path, status_json)
        .with_context(|| format!("writing {}", status_path.display()))?;

    let log_path = logs_dir.join("loader.log");
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

pub fn sync_from_handlers(config_dir: &Path) -> Result<Vec<RuntimeStatus>> {
    let handler_dirs = discover_handler_dirs(config_dir)?;
    let mut seen = HashSet::new();
    let mut statuses = Vec::new();

    for handler_dir in handler_dirs {
        let result = parse_handler(&handler_dir).and_then(|handler| {
            if !seen.insert(handler.id.clone()) {
                bail!("duplicate handler.id {}", handler.id);
            }

            let options = serde_json::to_value(handler.options)?;
            Spi::run_with_args(
                "INSERT INTO chainsync.jobs (name, options, status) VALUES ($1, $2, 'STOPPED') \
                 ON CONFLICT (name) DO UPDATE SET options = EXCLUDED.options, status = 'STOPPED'",
                &vec![
                    DatumWithOid::from(handler.id.clone()),
                    DatumWithOid::from(JsonB(options)),
                ],
            )?;

            Ok(handler.id)
        });

        match result {
            Ok(job_id) => {
                let status = RuntimeStatus {
                    job_id: job_id.clone(),
                    status: "LOADED".into(),
                    last_error: None,
                };
                write_handler_status(config_dir, &status)?;
                statuses.push(status);
            }
            Err(error) => {
                let fallback_id = handler_dir
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                let status = RuntimeStatus {
                    job_id: fallback_id,
                    status: "ERROR".into(),
                    last_error: Some(error.to_string()),
                };
                write_handler_status(config_dir, &status)?;
                statuses.push(status);
            }
        }
    }

    let mut stale_ids: Vec<String> = Vec::new();
    Spi::connect(|client| -> Result<(), pgrx::spi::Error> {
        let mut table =
            client.select("SELECT name FROM chainsync.jobs", None, &vec![])?;
        while table.next().is_some() {
            let name = table
                .get_by_name::<String, &'static str>("name")
                .unwrap()
                .unwrap();
            if !seen.contains(&name) {
                stale_ids.push(name);
            }
        }
        Ok(())
    })?;

    for stale in stale_ids {
        Spi::run_with_args(
            "DELETE FROM chainsync.jobs WHERE name = $1",
            &vec![DatumWithOid::from(stale.clone())],
        )?;
        let status = RuntimeStatus {
            job_id: stale,
            status: "REMOVED".into(),
            last_error: None,
        };
        write_handler_status(config_dir, &status)?;
        statuses.push(status);
    }

    Ok(statuses)
}
