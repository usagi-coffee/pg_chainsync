use std::collections::{HashMap, HashSet};
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
struct JobHeader {
    id: String,
    chain: String,
    mode: String,
    module: Option<String>,
}

#[derive(Deserialize, Clone)]
struct TomlJob {
    job: JobHeader,
    rpc: Option<String>,
    ws: Option<String>,
    preload: Option<bool>,
    oneshot: Option<bool>,
    cron: Option<String>,
    setup_handler: Option<String>,
    success_handler: Option<String>,
    failure_handler: Option<String>,
    evm: Option<crate::types::EvmOptions>,
    svm: Option<crate::types::SvmOptions>,
}

#[derive(Serialize)]
pub struct RuntimeStatus {
    pub job_id: String,
    pub status: String,
    pub last_error: Option<String>,
}

struct LoadedJob {
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

fn parse_job(path: &Path) -> Result<LoadedJob> {
    let source = fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let parsed: TomlJob = toml::from_str(&source)
        .with_context(|| format!("parsing TOML {}", path.display()))?;

    if parsed.job.id.trim().is_empty() {
        bail!("job.id is empty");
    }

    if parsed.job.chain != "evm" && parsed.job.chain != "svm" {
        bail!("job.chain must be one of: evm, svm");
    }
    if parsed.job.chain == "evm" && parsed.evm.is_none() {
        bail!("job.chain=evm requires [evm] section");
    }
    if parsed.job.chain == "svm" && parsed.svm.is_none() {
        bail!("job.chain=svm requires [svm] section");
    }

    let oneshot = match parsed.job.mode.as_str() {
        "oneshot" => Some(true),
        "stream" => Some(false),
        "cron" => Some(false),
        _ => bail!("job.mode must be one of: stream, oneshot, cron"),
    };

    if parsed.job.mode == "cron" && parsed.cron.is_none() {
        bail!("cron mode requires top-level cron value");
    }

    let setup_handler = parsed.setup_handler.map(Into::into);
    let success_handler = parsed.success_handler.map(Into::into);
    let failure_handler = parsed.failure_handler.map(Into::into);

    Ok(LoadedJob {
        id: parsed.job.id,
        options: JobOptions {
            rpc: resolve_opt(parsed.rpc)?,
            ws: resolve_opt(parsed.ws)?,
            preload: parsed.preload,
            oneshot: parsed.oneshot.or(oneshot),
            cron: resolve_opt(parsed.cron)?,
            setup_handler,
            success_handler,
            failure_handler,
            module: parsed.job.module.map(Into::into),
            evm: parsed.evm,
            svm: parsed.svm,
        },
    })
}

fn discover_toml_files(config_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(config_dir)
        .with_context(|| format!("reading dir {}", config_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("toml") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn discover_plugins(plugin_dir: &Path) -> Result<HashMap<String, PathBuf>> {
    let mut plugins = HashMap::new();
    for entry in fs::read_dir(plugin_dir).with_context(|| {
        format!("reading plugin dir {}", plugin_dir.display())
    })? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("so") {
            continue;
        }

        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            plugins.insert(stem.to_string(), path);
        }
    }
    Ok(plugins)
}

fn write_task_status(config_dir: &Path, status: &RuntimeStatus) -> Result<()> {
    let task_dir = config_dir.join("_runtime").join(&status.job_id);
    let logs_dir = task_dir.join("logs");
    fs::create_dir_all(&logs_dir).with_context(|| {
        format!("creating task runtime directory {}", task_dir.display())
    })?;

    let status_path = task_dir.join("status.json");
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

pub fn sync_from_toml(
    config_dir: &Path,
    plugin_dir: &Path,
) -> Result<Vec<RuntimeStatus>> {
    let files = discover_toml_files(config_dir)?;
    let plugins = discover_plugins(plugin_dir)?;
    let mut seen = HashSet::new();
    let mut statuses = Vec::new();

    for path in files {
        let result = parse_job(&path).and_then(|job| {
            if !seen.insert(job.id.clone()) {
                bail!("duplicate job.id {}", job.id);
            }

            if let Some(module) = &job.options.module {
                let Some(plugin_path) = plugins.get(module.as_ref()) else {
                    bail!("module {} was not found in plugin directory", module);
                };
                let metadata = plugin::validate_plugin(plugin_path)?;
                let _ = (metadata.abi_major, metadata.abi_minor, metadata.name, metadata.version);
            }

            let options = serde_json::to_value(job.options)?;
            Spi::run_with_args(
                "INSERT INTO chainsync.jobs (name, options, status) VALUES ($1, $2, 'STOPPED') \
                 ON CONFLICT (name) DO UPDATE SET options = EXCLUDED.options, status = 'STOPPED'",
                &vec![
                    DatumWithOid::from(job.id.clone()),
                    DatumWithOid::from(JsonB(options)),
                ],
            )?;
            Ok(job.id)
        });

        match result {
            Ok(job_id) => {
                let status = RuntimeStatus {
                    job_id: job_id.clone(),
                    status: "LOADED".into(),
                    last_error: None,
                };
                write_task_status(config_dir, &status)?;
                statuses.push(status);
            }
            Err(error) => {
                let job_id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                let status = RuntimeStatus {
                    job_id: job_id.clone(),
                    status: "ERROR".into(),
                    last_error: Some(error.to_string()),
                };
                write_task_status(config_dir, &status)?;
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
            job_id: stale.clone(),
            status: "REMOVED".into(),
            last_error: None,
        };
        write_task_status(config_dir, &status)?;
        statuses.push(status);
    }

    Ok(statuses)
}
