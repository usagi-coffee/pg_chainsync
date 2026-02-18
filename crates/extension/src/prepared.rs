use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{OnceLock, RwLock};

use anyhow::{Context, Result};
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use serde_json::Value;

use crate::types::HandlerRuntime;

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

fn stable_name(handler_id: i64, kind: &str, query_id: &str) -> String {
    let mut hasher = DefaultHasher::new();
    kind.hash(&mut hasher);
    query_id.hash(&mut hasher);
    let digest = hasher.finish();
    format!("chainsync_q_{}_{}_{}", handler_id, kind, digest)
}

#[derive(Default, Clone)]
struct HandlerPrepared {
    lookups: HashMap<String, String>,
    mutations: HashMap<String, String>,
}

static PREPARED: OnceLock<RwLock<HashMap<i64, HandlerPrepared>>> = OnceLock::new();

fn prepared_store() -> &'static RwLock<HashMap<i64, HandlerPrepared>> {
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

pub fn prepare_handler_queries(handler: &HandlerRuntime) -> Result<usize> {
    let mut prepared = 0usize;

    let mut handler_prepared = HandlerPrepared::default();

    if let Some(lookups) = &handler.options.lookup_queries {
        for (query_id, sql_text) in lookups {
            let sql = resolve_env_sql(sql_text)
                .with_context(|| format!("resolving lookup sql '{}'", query_id))?;
            let name = stable_name(handler.id, "lookup", query_id);
            prepare_lookup_statement(&name, &sql)?;
            handler_prepared.lookups.insert(query_id.clone(), name);
            prepared += 1;
        }
    }

    if let Some(mutations) = &handler.options.mutation_queries {
        for (query_id, sql_text) in mutations {
            let sql = resolve_env_sql(sql_text).with_context(|| {
                format!("resolving mutation sql '{}'", query_id)
            })?;
            let name = stable_name(handler.id, "mutation", query_id);
            prepare_named_statement(&name, &sql)?;
            handler_prepared.mutations.insert(query_id.clone(), name);
            prepared += 1;
        }
    }

    prepared_store()
        .write()
        .expect("prepared plans lock")
        .insert(handler.id, handler_prepared);

    Ok(prepared)
}

pub fn prepare_all_handlers(handlers: &[HandlerRuntime]) -> Result<usize> {
    let mut total = 0usize;
    prepared_store()
        .write()
        .expect("prepared plans lock")
        .clear();
    for handler in handlers {
        total += prepare_handler_queries(handler)
            .with_context(|| format!("preparing queries for {}", handler.name))?;
    }
    Ok(total)
}

pub fn execute_mutation(
    handler_id: i64,
    mutation_id: &str,
    payload: JsonB,
) -> Result<()> {
    let sql_name = {
        let guard = prepared_store().read().expect("prepared plans lock");
        let Some(handler) = guard.get(&handler_id) else {
            anyhow::bail!("no prepared queries for handler {}", handler_id);
        };
        let Some(name) = handler.mutations.get(mutation_id) else {
            anyhow::bail!(
                "mutation id '{}' not prepared for handler {}",
                mutation_id,
                handler_id
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

pub fn execute_lookup(handler_id: i64, lookup_id: &str) -> Result<Value> {
    let sql_name = {
        let guard = prepared_store().read().expect("prepared plans lock");
        let Some(handler) = guard.get(&handler_id) else {
            anyhow::bail!("no prepared queries for handler {}", handler_id);
        };
        let Some(name) = handler.lookups.get(lookup_id) else {
            anyhow::bail!(
                "lookup id '{}' not prepared for handler {}",
                lookup_id,
                handler_id
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
