use std::collections::HashMap;
use std::fs;
use std::sync::Mutex;

use anyhow::{bail, Context, Result};
use once_cell::sync::Lazy;
use pg_chainsync_sdk::{
    export_plugin, ModuleResponse, Mutation, PluginHandler,
};
use serde::{Deserialize, Serialize};

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
    handler_id: Option<i64>,
    state_path: Option<String>,
    prefetched: Option<serde_json::Value>,
    address: Option<String>,
    data: Option<String>,
    ingest_unix: Option<u64>,
}

struct OhlcHandler;

static STATE: Lazy<Mutex<State>> = Lazy::new(|| Mutex::new(State::default()));

fn state_path(handler_id: i64, event_state_path: Option<&str>) -> String {
    if let Some(path) = event_state_path {
        return path.to_string();
    }

    if let Ok(path) = std::env::var("CHAINSYNC_STATE_PATH") {
        return path;
    }

    if let Ok(root) = std::env::var("CHAINSYNC_STATE_ROOT") {
        return format!("{}/{}_state.json", root, handler_id);
    }

    format!("/tmp/ohlc_state_{}.json", handler_id)
}

fn load_state_if_needed(
    handler_id: i64,
    event_state_path: Option<&str>,
) -> Result<()> {
    let path = state_path(handler_id, event_state_path);
    let content = match fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };

    let parsed: State = serde_json::from_str(&content)
        .with_context(|| format!("parsing state {}", path))?;
    *STATE.lock().expect("state lock") = parsed;
    Ok(())
}

fn persist_state(handler_id: i64, event_state_path: Option<&str>) -> Result<()> {
    let path = state_path(handler_id, event_state_path);
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
    let amount1_in =
        u128::from_str_radix(&hex[64..128], 16).unwrap_or(0) as f64;
    let amount0_out =
        u128::from_str_radix(&hex[128..192], 16).unwrap_or(0) as f64;
    let amount1_out =
        u128::from_str_radix(&hex[192..256], 16).unwrap_or(0) as f64;

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

fn process_event(input: serde_json::Value) -> Result<ModuleResponse> {
    let event: InputEvent = serde_json::from_value(input)?;
    let handler_id = event.handler_id.unwrap_or(0);
    load_state_if_needed(handler_id, event.state_path.as_deref())?;

    if event.event != "evm_log" {
        return Ok(ModuleResponse::Ignore);
    }

    let pair_id = event.address.unwrap_or_else(|| "unknown_pair".into());
    let data = match event.data {
        Some(v) => v,
        None => return Ok(ModuleResponse::Ignore),
    };
    let now = event.ingest_unix.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    });

    let (base_decimals, quote_decimals) =
        lookup_decimals(event.prefetched.as_ref());
    let (price, vol_base, vol_quote) =
        decode_swap_price_and_volume(&data, base_decimals, quote_decimals)?;
    let bucket_start = (now / 60) * 60;

    let mut to_emit: Option<serde_json::Value> = None;

    {
        let mut state = STATE.lock().expect("state lock");
        let entry = state.active.entry(pair_id.clone()).or_insert_with(|| {
            CandleState {
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
            }
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

    persist_state(handler_id, event.state_path.as_deref())?;

    match to_emit {
        Some(payload) => Ok(ModuleResponse::Done {
            mutations: vec![Mutation {
                id: "upsert_ohlc_1m".into(),
                payload,
            }],
        }),
        None => Ok(ModuleResponse::Ignore),
    }
}

impl PluginHandler for OhlcHandler {
    fn handle_event(
        input: serde_json::Value,
    ) -> Result<ModuleResponse, String> {
        process_event(input).map_err(|e| e.to_string())
    }
}

export_plugin!(
    OhlcHandler,
    "ohlc_handler",
    "0.1.0",
    include_str!("../handler.toml")
);

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
            "handler_id": 42,
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
        let out1 = process_event(first).expect("handle first");
        assert!(matches!(out1, ModuleResponse::Ignore));

        let second = sample_event(1_700_000_061, &state_path);
        let out2 = process_event(second).expect("handle second");

        match out2 {
            ModuleResponse::Done { mutations } => {
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
