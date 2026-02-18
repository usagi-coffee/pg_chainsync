use std::collections::HashMap;
use std::ffi::c_char;
use std::fs;
use std::sync::Mutex;

use anyhow::{Context, Result, bail};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[repr(C)]
pub struct PluginMetadataV1 {
    pub abi_major: u16,
    pub abi_minor: u16,
    pub name: *const c_char,
    pub version: *const c_char,
}

unsafe impl Sync for PluginMetadataV1 {}

static NAME: &[u8] = b"ohlc_handler\0";
static VERSION: &[u8] = b"0.1.0\0";

#[no_mangle]
pub extern "C" fn chainsync_plugin_meta_v1() -> *const PluginMetadataV1 {
    static META: PluginMetadataV1 = PluginMetadataV1 {
        abi_major: 1,
        abi_minor: 0,
        name: NAME.as_ptr() as *const c_char,
        version: VERSION.as_ptr() as *const c_char,
    };
    &META
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_base: f64,
    volume_quote: f64,
    trades: i64,
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    active: HashMap<String, CandleState>,
}

#[derive(Serialize, Deserialize, Clone)]
struct CandleState {
    bucket_start_unix: u64,
    candle: Candle,
}

#[derive(Deserialize)]
struct InputEvent {
    event: String,
    job_id: Option<i64>,
    state_path: Option<String>,
    prefetched: Option<serde_json::Value>,
    address: Option<String>,
    data: Option<String>,
    ingest_unix: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Output {
    Ignore,
    Error { message: String },
    Done { mutations: Vec<Mutation> },
}

#[derive(Serialize, Deserialize, Debug)]
struct Mutation {
    id: String,
    payload: serde_json::Value,
}

static STATE: Lazy<Mutex<State>> = Lazy::new(|| Mutex::new(State::default()));

fn state_path(job_id: i64, event_state_path: Option<&str>) -> String {
    if let Some(path) = event_state_path {
        return path.to_string();
    }

    if let Ok(path) = std::env::var("CHAINSYNC_STATE_PATH") {
        return path;
    }

    if let Ok(root) = std::env::var("CHAINSYNC_STATE_ROOT") {
        return format!("{}/{}_state.json", root, job_id);
    }

    format!("/tmp/ohlc_state_{}.json", job_id)
}

fn load_state_if_needed(job_id: i64, event_state_path: Option<&str>) -> Result<()> {
    let path = state_path(job_id, event_state_path);
    let content = match fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };

    let parsed: State = serde_json::from_str(&content)
        .with_context(|| format!("parsing state {}", path))?;
    *STATE.lock().expect("state lock") = parsed;
    Ok(())
}

fn persist_state(job_id: i64, event_state_path: Option<&str>) -> Result<()> {
    let path = state_path(job_id, event_state_path);
    let tmp = format!("{}.tmp", path);

    let guard = STATE.lock().expect("state lock");
    let data = serde_json::to_vec_pretty(&*guard)?;
    drop(guard);

    fs::write(&tmp, data).with_context(|| format!("writing {}", tmp))?;
    fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} -> {}", tmp, path))?;
    Ok(())
}

fn lookup_decimals(prefetched: Option<&serde_json::Value>) -> (u32, u32) {
    let Some(prefetched) = prefetched else {
        return (0, 0);
    };
    let Some(pool_meta) = prefetched.get("pool_meta") else {
        return (0, 0);
    };
    let Some(rows) = pool_meta.as_array() else {
        return (0, 0);
    };
    let Some(first) = rows.first() else {
        return (0, 0);
    };

    let base = first
        .get("base_decimals")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(0);
    let quote = first
        .get("quote_decimals")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(0);
    (base, quote)
}

fn decode_swap_price_and_volume(
    data_hex: &str,
    base_decimals: u32,
    quote_decimals: u32,
) -> Result<(f64, f64, f64)> {
    let hex = data_hex.strip_prefix("0x").unwrap_or(data_hex);
    if hex.len() < 64 * 4 {
        bail!("swap data is too short");
    }

    let amount0_in = u128::from_str_radix(&hex[0..64], 16).unwrap_or(0) as f64;
    let amount1_in = u128::from_str_radix(&hex[64..128], 16).unwrap_or(0) as f64;
    let amount0_out = u128::from_str_radix(&hex[128..192], 16).unwrap_or(0) as f64;
    let amount1_out = u128::from_str_radix(&hex[192..256], 16).unwrap_or(0) as f64;

    let base_raw = amount0_in + amount0_out;
    let quote_raw = amount1_in + amount1_out;
    let base_scale = 10f64.powi(base_decimals as i32);
    let quote_scale = 10f64.powi(quote_decimals as i32);
    let base = base_raw / base_scale;
    let quote = quote_raw / quote_scale;
    if base <= 0.0 || quote <= 0.0 {
        bail!("invalid swap volumes");
    }

    Ok((quote / base, base, quote))
}

fn handle_event(bytes: &[u8]) -> Result<Output> {
    let event: InputEvent = serde_json::from_slice(bytes)?;
    let job_id = event.job_id.unwrap_or(0);
    load_state_if_needed(job_id, event.state_path.as_deref())?;

    if event.event != "evm_log" {
        return Ok(Output::Ignore);
    }

    let pair_id = event.address.unwrap_or_else(|| "unknown_pair".into());
    let data = match event.data {
        Some(v) => v,
        None => return Ok(Output::Ignore),
    };
    let now = event.ingest_unix.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    });

    let (base_decimals, quote_decimals) =
        lookup_decimals(event.prefetched.as_ref());
    let (price, vol_base, vol_quote) = decode_swap_price_and_volume(
        &data,
        base_decimals,
        quote_decimals,
    )?;
    let bucket_start = (now / 60) * 60;

    let mut to_emit: Option<serde_json::Value> = None;

    {
        let mut state = STATE.lock().expect("state lock");
        let entry = state.active.entry(pair_id.clone()).or_insert_with(|| CandleState {
            bucket_start_unix: bucket_start,
            candle: Candle {
                open: price,
                high: price,
                low: price,
                close: price,
                volume_base: 0.0,
                volume_quote: 0.0,
                trades: 0,
            },
        });

        if entry.bucket_start_unix != bucket_start {
            to_emit = Some(serde_json::json!({
                "pair_id": pair_id,
                "bucket_start_unix": entry.bucket_start_unix,
                "open": entry.candle.open,
                "high": entry.candle.high,
                "low": entry.candle.low,
                "close": entry.candle.close,
                "volume_base": entry.candle.volume_base,
                "volume_quote": entry.candle.volume_quote,
                "trades": entry.candle.trades,
            }));

            *entry = CandleState {
                bucket_start_unix: bucket_start,
                candle: Candle {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume_base: 0.0,
                    volume_quote: 0.0,
                    trades: 0,
                },
            };
        }

        let candle = &mut entry.candle;
        candle.high = candle.high.max(price);
        candle.low = candle.low.min(price);
        candle.close = price;
        candle.volume_base += vol_base;
        candle.volume_quote += vol_quote;
        candle.trades += 1;
    }

    persist_state(job_id, event.state_path.as_deref())?;

    match to_emit {
        Some(payload) => Ok(Output::Done {
            mutations: vec![Mutation {
                id: "upsert_ohlc_1m".into(),
                payload,
            }],
        }),
        None => Ok(Output::Ignore),
    }
}

#[no_mangle]
pub extern "C" fn chainsync_handle_event_v1(
    input_ptr: *const u8,
    input_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if input_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
        return 1;
    }

    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
    let output = match handle_event(input) {
        Ok(v) => v,
        Err(err) => Output::Error {
            message: err.to_string(),
        },
    };

    let encoded = match serde_json::to_vec(&output) {
        Ok(v) => v,
        Err(_) => return 2,
    };

    let mut boxed = encoded.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    let len = boxed.len();
    std::mem::forget(boxed);

    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }

    0
}

#[no_mangle]
pub extern "C" fn chainsync_plugin_free_buffer(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }

    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_word(value: u128) -> String {
        format!("{value:064x}")
    }

    fn swap_data(
        amount0_in: u128,
        amount1_in: u128,
        amount0_out: u128,
        amount1_out: u128,
    ) -> String {
        format!(
            "{}{}{}{}",
            encode_word(amount0_in),
            encode_word(amount1_in),
            encode_word(amount0_out),
            encode_word(amount1_out)
        )
    }

    fn sample_event(
        ingest_unix: u64,
        state_path: &std::path::Path,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": "evm_log",
            "job_id": 42,
            "state_path": state_path.to_string_lossy(),
            "prefetched": {
                "pool_meta": [
                    { "base_decimals": 2, "quote_decimals": 2 }
                ]
            },
            "address": "0xpool1",
            "data": swap_data(100, 1000, 0, 0),
            "ingest_unix": ingest_unix
        })
    }

    #[test]
    fn emits_done_on_minute_rollover() {
        let state_path = std::env::temp_dir().join(format!(
            "ohlc_handler_test_{}_state.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&state_path);

        let first = sample_event(1_700_000_000, &state_path);
        let out1 = handle_event(
            serde_json::to_vec(&first).expect("encode first").as_slice(),
        )
        .expect("handle first");
        assert!(matches!(out1, Output::Ignore));

        let second = sample_event(1_700_000_061, &state_path);
        let out2 = handle_event(
            serde_json::to_vec(&second)
                .expect("encode second")
                .as_slice(),
        )
        .expect("handle second");

        match out2 {
            Output::Done { mutations } => {
                assert_eq!(mutations.len(), 1);
                assert_eq!(mutations[0].id, "upsert_ohlc_1m");
                assert_eq!(
                    mutations[0].payload["pair_id"].as_str(),
                    Some("0xpool1")
                );
                assert_eq!(
                    mutations[0].payload["volume_base"].as_f64(),
                    Some(1.0)
                );
                assert_eq!(
                    mutations[0].payload["volume_quote"].as_f64(),
                    Some(10.0)
                );
            }
            other => panic!("expected done, got {:?}", other),
        }

        let _ = std::fs::remove_file(&state_path);
    }
}
