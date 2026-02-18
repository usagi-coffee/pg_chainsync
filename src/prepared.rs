use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{OnceLock, RwLock};

use anyhow::{Context, Result};
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use serde_json::Value;

use crate::types::Job;

fn resolve_env_sql(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut index = 0usize;

    while let Some(start_rel) = input[index..].find("${") {
        let start = index + start_rel;
        out.push_str(&input[index..start]);
        let Some(end_rel) = input[start + 2..].find('}') else {
            anyhow::bail!("unterminated env placeholder in sql");
        };
        let end = start + 2 + end_rel;
        let key = &input[start + 2..end];
        let value = std::env::var(key)
            .with_context(|| format!("missing environment variable {}", key))?;
        out.push_str(&value);
        index = end + 1;
    }

    out.push_str(&input[index..]);
    Ok(out)
}

fn stable_name(job_id: i64, kind: &str, query_id: &str) -> String {
    let mut hasher = DefaultHasher::new();
    kind.hash(&mut hasher);
    query_id.hash(&mut hasher);
    let digest = hasher.finish();
    format!("chainsync_q_{}_{}_{}", job_id, kind, digest)
}

#[derive(Default, Clone)]
struct JobPrepared {
    lookups: HashMap<String, String>,
    mutations: HashMap<String, String>,
}

static PREPARED: OnceLock<RwLock<HashMap<i64, JobPrepared>>> = OnceLock::new();

fn prepared_store() -> &'static RwLock<HashMap<i64, JobPrepared>> {
    PREPARED.get_or_init(|| RwLock::new(HashMap::new()))
}

fn prepare_named_statement(name: &str, sql: &str) -> Result<()> {
    let _ = Spi::run(&format!("DEALLOCATE {}", name));
    Spi::run(&format!("PREPARE {} AS {}", name, sql))
        .with_context(|| format!("preparing statement {}", name))?;
    Ok(())
}

fn prepare_lookup_statement(name: &str, sql: &str) -> Result<()> {
    let normalized = sql.trim().trim_end_matches(';');
    let wrapped = format!(
        "SELECT COALESCE(jsonb_agg(t), '[]'::jsonb) FROM ({}) AS t",
        normalized
    );
    prepare_named_statement(name, &wrapped)
}

pub fn prepare_job_queries(job: &Job) -> Result<usize> {
    let Some(handler_dir) = &job.options.handler_dir else {
        return Ok(0);
    };
    let base = PathBuf::from(handler_dir);
    let mut prepared = 0usize;

    let mut job_prepared = JobPrepared::default();

    if let Some(lookups) = &job.options.lookup_queries {
        for (query_id, rel_path) in lookups {
            let sql_path = base.join(rel_path);
            let sql = std::fs::read_to_string(&sql_path)
                .with_context(|| format!("reading {}", sql_path.display()))?;
            let sql = resolve_env_sql(&sql)
                .with_context(|| format!("resolving {}", sql_path.display()))?;
            let name = stable_name(job.id, "lookup", query_id);
            prepare_lookup_statement(&name, &sql)?;
            job_prepared.lookups.insert(query_id.clone(), name);
            prepared += 1;
        }
    }

    if let Some(mutations) = &job.options.mutation_queries {
        for (query_id, rel_path) in mutations {
            let sql_path = base.join(rel_path);
            let sql = std::fs::read_to_string(&sql_path)
                .with_context(|| format!("reading {}", sql_path.display()))?;
            let sql = resolve_env_sql(&sql)
                .with_context(|| format!("resolving {}", sql_path.display()))?;
            let name = stable_name(job.id, "mutation", query_id);
            prepare_named_statement(&name, &sql)?;
            job_prepared.mutations.insert(query_id.clone(), name);
            prepared += 1;
        }
    }

    prepared_store()
        .write()
        .expect("prepared plans lock")
        .insert(job.id, job_prepared);

    Ok(prepared)
}

pub fn prepare_all_jobs(jobs: &[Job]) -> Result<usize> {
    let mut total = 0usize;
    prepared_store()
        .write()
        .expect("prepared plans lock")
        .clear();
    for job in jobs {
        total += prepare_job_queries(job)
            .with_context(|| format!("preparing queries for {}", job.name))?;
    }
    Ok(total)
}

pub fn execute_mutation(
    job_id: i64,
    mutation_id: &str,
    payload: JsonB,
) -> Result<()> {
    let sql_name = {
        let guard = prepared_store().read().expect("prepared plans lock");
        let Some(job) = guard.get(&job_id) else {
            anyhow::bail!("no prepared queries for job {}", job_id);
        };
        let Some(name) = job.mutations.get(mutation_id) else {
            anyhow::bail!(
                "mutation id '{}' not prepared for job {}",
                mutation_id,
                job_id
            );
        };
        name.clone()
    };

    Spi::run_with_args(
        format!("EXECUTE {}($1)", sql_name).as_str(),
        &vec![DatumWithOid::from(payload)],
    )
    .with_context(|| format!("executing mutation {}", mutation_id))?;
    Ok(())
}

pub fn execute_lookup(job_id: i64, lookup_id: &str) -> Result<Value> {
    let sql_name = {
        let guard = prepared_store().read().expect("prepared plans lock");
        let Some(job) = guard.get(&job_id) else {
            anyhow::bail!("no prepared queries for job {}", job_id);
        };
        let Some(name) = job.lookups.get(lookup_id) else {
            anyhow::bail!(
                "lookup id '{}' not prepared for job {}",
                lookup_id,
                job_id
            );
        };
        name.clone()
    };

    let result =
        Spi::get_one::<JsonB>(format!("EXECUTE {}", sql_name).as_str())
            .with_context(|| format!("executing lookup {}", lookup_id))?
            .context("lookup did not return JSON value")?;
    Ok(result.0)
}
