use std::fs;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use pg_chainsync_sdk::{
    export_plugin, plugin_log, ModuleResponse, Mutation, PluginHandler,
    SetupResponse,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::{json, Value};

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, Deserialize)]
struct CronInput {
    event: String,
    state_path: Option<String>,
    rpc: Option<String>,
    svm: Option<SvmInput>,
    plugin: Option<PluginConfig>,
}

#[derive(Debug, Deserialize)]
struct SvmInput {
    program: Option<String>,
    accounts_filters: Option<Value>,
    accounts_data_slice: Option<DataSlice>,
}

#[derive(Debug, Deserialize)]
struct DataSlice {
    offset: u64,
    length: u64,
}

#[derive(Debug, Default, Deserialize)]
struct PluginConfig {
    limit: Option<u64>,
    decimals: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ProgramAccountsPage {
    accounts: Vec<AccountRow>,
    #[serde(rename = "paginationKey")]
    pagination_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AccountRow {
    pubkey: String,
    account: AccountInfo,
}

#[derive(Debug, Deserialize)]
struct AccountInfo {
    data: Value,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    changed_since_slot: Option<u64>,
}

struct SvmAccountsHandler;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn rpc_call<T: DeserializeOwned>(
    rpc_url: &str,
    method: &str,
    params: Value,
) -> Result<T, String> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": "1",
        "method": method,
        "params": params,
    });

    let response = ureq::post(rpc_url)
        .set("Content-Type", "application/json")
        .send_json(req)
        .map_err(|error| match error {
            ureq::Error::Status(status, resp) => {
                let body = resp.into_string().unwrap_or_default();
                format!("rpc status {}: {}", status, body)
            }
            other => format!("rpc transport error: {}", other),
        })?;

    let body: RpcEnvelope<T> = response
        .into_json()
        .map_err(|error| format!("rpc decode error: {}", error))?;

    if let Some(err) = body.error {
        return Err(format!(
            "rpc error code={} message={} data={}",
            err.code,
            err.message,
            err.data.unwrap_or(Value::Null)
        ));
    }
    body.result
        .ok_or_else(|| "rpc response missing result".to_string())
}

fn get_slot(rpc_url: &str) -> Result<u64, String> {
    rpc_call(rpc_url, "getSlot", json!([{ "commitment": "finalized" }]))
}

fn parse_amount(account_data: &Value) -> Option<u64> {
    let data_b64 = if let Some(array) = account_data.as_array() {
        array.first().and_then(Value::as_str)
    } else {
        account_data.as_str()
    }?;

    let decoded = BASE64.decode(data_b64.as_bytes()).ok()?;
    if decoded.len() < 8 {
        return None;
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&decoded[..8]);
    Some(u64::from_le_bytes(bytes))
}

fn load_state(path: Option<&str>) -> State {
    let Some(path) = path else {
        return State::default();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return State::default();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_state(path: Option<&str>, state: &State) -> Result<(), String> {
    let Some(path) = path else {
        return Ok(());
    };
    let tmp = format!("{}.tmp", path);
    let data = serde_json::to_vec(state)
        .map_err(|error| format!("state encode error: {}", error))?;
    fs::write(&tmp, data)
        .map_err(|error| format!("state write error ({}): {}", tmp, error))?;
    fs::rename(&tmp, path).map_err(|error| {
        format!("state rename error ({} -> {}): {}", tmp, path, error)
    })?;
    Ok(())
}

fn build_options(
    svm: &SvmInput,
    limit: u64,
    changed_since_slot: Option<u64>,
    pagination_key: Option<&str>,
) -> Value {
    let mut options = json!({
        "encoding": "base64",
        "limit": limit,
    });
    if let Some(filters) = &svm.accounts_filters {
        options["filters"] = filters.clone();
    }
    if let Some(data_slice) = &svm.accounts_data_slice {
        options["dataSlice"] = json!({
            "offset": data_slice.offset,
            "length": data_slice.length,
        });
    }
    if let Some(slot) = changed_since_slot {
        options["changedSinceSlot"] = json!(slot);
    }
    if let Some(key) = pagination_key {
        options["paginationKey"] = json!(key);
    }
    options
}

fn process(input: Value) -> Result<ModuleResponse, String> {
    let event: CronInput =
        serde_json::from_value(input).map_err(|error| error.to_string())?;

    if event.event != "cron_tick" {
        return Ok(ModuleResponse::Ignore);
    }

    let rpc_url = event
        .rpc
        .ok_or_else(|| "missing rpc in cron payload".to_string())?;
    let svm = event
        .svm
        .ok_or_else(|| "missing svm config in cron payload".to_string())?;
    let program = svm
        .program
        .as_deref()
        .unwrap_or(TOKEN_PROGRAM_ID)
        .to_string();

    let plugin_cfg = event.plugin.unwrap_or_default();
    let limit = plugin_cfg
        .limit
        .unwrap_or_else(|| env_u64("SVM_ACCOUNTS_LIMIT", 2_000))
        .clamp(1, 10_000);
    let decimals = plugin_cfg
        .decimals
        .unwrap_or_else(|| env_u32("SVM_ACCOUNTS_DECIMALS", 0));

    let mut state = load_state(event.state_path.as_deref());
    let watermark = get_slot(&rpc_url)?;

    let mut pagination_key: Option<String> = None;
    let mut pages = 0usize;
    let mut processed = 0usize;
    let mut mutations = Vec::new();

    loop {
        let options = build_options(
            &svm,
            limit,
            state.changed_since_slot,
            pagination_key.as_deref(),
        );
        let page: ProgramAccountsPage = rpc_call(
            &rpc_url,
            "getProgramAccountsV2",
            json!([program, options]),
        )?;

        for row in &page.accounts {
            let Some(balance) = parse_amount(&row.account.data) else {
                continue;
            };
            mutations.push(Mutation {
                id: "upsert_account".to_string(),
                payload: json!({
                    "address": row.pubkey,
                    "balance": balance.to_string(),
                    "decimals": decimals,
                }),
            });
        }

        processed += page.accounts.len();
        pages += 1;
        pagination_key = page.pagination_key;

        if pagination_key.is_none() {
            break;
        }
        if pages > 100_000 {
            return Err("pagination safety limit reached".to_string());
        }
    }

    state.changed_since_slot = Some(watermark);
    save_state(event.state_path.as_deref(), &state)?;

    plugin_log!(
        "cron sync complete program={} pages={} accounts={} changed_since_slot={} mutations={}",
        program,
        pages,
        processed,
        watermark,
        mutations.len()
    );

    if mutations.is_empty() {
        Ok(ModuleResponse::Ignore)
    } else {
        Ok(ModuleResponse::Done { mutations })
    }
}

impl PluginHandler for SvmAccountsHandler {
    fn setup(input: Value) -> Result<SetupResponse, String> {
        let state_path = input
            .get("state_path")
            .and_then(Value::as_str)
            .unwrap_or("<none>");
        let plugin_options = input
            .get("plugin")
            .cloned()
            .unwrap_or(Value::Null)
            .to_string();
        plugin_log!(
            "setup state_path={} plugin={}",
            state_path,
            plugin_options
        );
        Ok(SetupResponse::default())
    }

    fn handle(input: Value) -> Result<ModuleResponse, String> {
        process(input)
    }
}

export_plugin!(SvmAccountsHandler, "svm_accounts_handler", "0.1.0");
