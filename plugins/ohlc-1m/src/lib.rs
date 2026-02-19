use std::collections::HashMap;
use std::fs;
use std::sync::Mutex;

use anyhow::{bail, Context, Result};
use once_cell::sync::Lazy;
use pg_chainsync_sdk::{
    export_plugin, plugin_log, ModuleResponse, Mutation, PluginHandler,
    SetupResponse,
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

    format!("/tmp/chainsync_ohlc_{}.json", handler_id)
}

fn load_state_if_needed(
    handler_id: i64,
    event_state_path: Option<&str>,
) -> Result<()> {
    let path = state_path(handler_id, event_state_path);
    let content = match fs::read_to_string(&path) {
        Ok(v) => {
            plugin_log!("loading state path={}", path);
            v
        }
        Err(_) => {
            plugin_log!("no existing state path={}", path);
            return Ok(());
        }
    };

    let parsed: State = serde_json::from_str(&content)
        .with_context(|| format!("parsing state {}", path))?;
    *STATE.lock().expect("state lock") = parsed;
    Ok(())
}

fn persist_state(
    handler_id: i64,
    event_state_path: Option<&str>,
) -> Result<()> {
    let path = state_path(handler_id, event_state_path);
    let tmp = format!("{}.tmp", path);

    let guard = STATE.lock().expect("state lock");
    let data = serde_json::to_vec_pretty(&*guard)?;
    drop(guard);

    fs::write(&tmp, data).with_context(|| format!("writing {}", tmp))?;
    fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} -> {}", tmp, path))?;
    plugin_log!("persisted state path={}", path);
    Ok(())
}

fn lookup_decimals(prefetched: Option<&serde_json::Value>) -> (u32, u32) {
    let Some(prefetched) = prefetched else {
        plugin_log!("prefetched missing; using decimals 0/0");
        return (0, 0);
    };
    let Some(pool_meta) = prefetched
        .get("pool_meta")
        .or_else(|| prefetched.get("decimals"))
    else {
        plugin_log!(
            "prelookup result missing 'pool_meta'/'decimals'; using decimals 0/0"
        );
        return (0, 0);
    };
    let Some(rows) = pool_meta.as_array() else {
        plugin_log!("prelookup rows malformed; using decimals 0/0");
        return (0, 0);
    };
    let Some(first) = rows.first() else {
        plugin_log!("prelookup rows empty; using decimals 0/0");
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
        plugin_log!("ignore non-evm_log event={}", event.event);
        return Ok(ModuleResponse::Ignore);
    }

    let pair_id = event.address.unwrap_or_else(|| "unknown_pair".into());
    let data = match event.data {
        Some(v) => v,
        None => {
            plugin_log!("ignore missing data pair_id={}", pair_id);
            return Ok(ModuleResponse::Ignore);
        }
    };
    let now = event.ingest_unix.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    });

    let (base_decimals, quote_decimals) =
        lookup_decimals(event.prefetched.as_ref());
    plugin_log!(
        "pair={} decimals base={} quote={}",
        pair_id,
        base_decimals,
        quote_decimals
    );
    let (price, vol_base, vol_quote) =
        decode_swap_price_and_volume(&data, base_decimals, quote_decimals)?;
    plugin_log!(
        "decoded pair={} price={} volume_base={} volume_quote={}",
        pair_id,
        price,
        vol_base,
        vol_quote
    );
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
            plugin_log!(
                "rollover pair={} emit_bucket={} new_bucket={}",
                pair_id,
                to_emit
                    .as_ref()
                    .and_then(|v| v.get("bucket_start_unix"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                bucket_start
            );
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
        None => {
            plugin_log!("no closed candle yet");
            Ok(ModuleResponse::Ignore)
        }
    }
}

impl PluginHandler for OhlcHandler {
    fn setup(input: serde_json::Value) -> Result<SetupResponse, String> {
        let state_path = input
            .get("state_path")
            .and_then(|v| v.as_str())
            .map(ToString::to_string);

        let should_reset = std::env::var("CHAINSYNC_OHLC_RESET_STATE_ON_SETUP")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if let Some(path) = state_path {
            plugin_log!("setup state_path={}", path);
            if should_reset {
                match fs::remove_file(&path) {
                    Ok(()) => {
                        plugin_log!("setup removed stale state file {}", path)
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        plugin_log!("setup state file already absent {}", path)
                    }
                    Err(err) => {
                        return Err(format!(
                            "failed to remove state file '{}': {}",
                            path, err
                        ));
                    }
                }
            }
        } else {
            plugin_log!("setup: no state_path provided");
        }

        Ok(SetupResponse::default())
    }

    fn handle_event(
        input: serde_json::Value,
    ) -> Result<ModuleResponse, String> {
        match process_event(input) {
            Ok(response) => Ok(response),
            Err(error) => {
                plugin_log!("handler error: {}", error);
                Err(error.to_string())
            }
        }
    }
}

export_plugin!(OhlcHandler, "ohlc_handler", "0.1.0");

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::MutexGuard;

    static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

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

    fn lock_test_state() -> MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        STATE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            .clear();
        guard
    }

    #[test]
    fn emits_done_on_minute_rollover() {
        let _guard = lock_test_state();
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

    #[test]
    fn ignores_non_evm_log() {
        let _guard = lock_test_state();
        let state_path = std::env::temp_dir().join(format!(
            "ohlc_handler_test_non_evm_{}_state.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&state_path);

        let mut event = sample_event(1_700_000_000, &state_path);
        event["event"] = serde_json::json!("svm_log");
        let out = process_event(event).expect("process event");
        assert!(matches!(out, ModuleResponse::Ignore));

        let _ = std::fs::remove_file(&state_path);
    }

    #[test]
    fn ignores_missing_data() {
        let _guard = lock_test_state();
        let state_path = std::env::temp_dir().join(format!(
            "ohlc_handler_test_missing_data_{}_state.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&state_path);

        let mut event = sample_event(1_700_000_000, &state_path);
        event["data"] = serde_json::Value::Null;
        let out = process_event(event).expect("process event");
        assert!(matches!(out, ModuleResponse::Ignore));

        let _ = std::fs::remove_file(&state_path);
    }

    #[test]
    fn errors_on_short_swap_payload() {
        let _guard = lock_test_state();
        let state_path = std::env::temp_dir().join(format!(
            "ohlc_handler_test_short_payload_{}_state.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&state_path);

        let mut event = sample_event(1_700_000_000, &state_path);
        event["data"] = serde_json::json!("00ff");
        let err = process_event(event).expect_err("expected decode error");
        assert!(err.to_string().contains("swap data is too short"));

        let _ = std::fs::remove_file(&state_path);
    }

    #[test]
    fn loads_persisted_state_and_rolls_over() {
        let _guard = lock_test_state();
        let state_path = std::env::temp_dir().join(format!(
            "ohlc_handler_test_reload_state_{}_state.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&state_path);

        let first = sample_event(1_700_000_000, &state_path);
        let out1 = process_event(first).expect("first event");
        assert!(matches!(out1, ModuleResponse::Ignore));

        STATE.lock().expect("state lock").active.clear();

        let second = sample_event(1_700_000_061, &state_path);
        let out2 = process_event(second).expect("second event");

        let ModuleResponse::Done { mutations } = out2 else {
            panic!("expected done");
        };
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].id, "upsert_ohlc_1m");
        assert_eq!(
            mutations[0].payload["bucket_start_unix"].as_u64(),
            Some(1_699_999_980)
        );
        assert_eq!(mutations[0].payload["open"].as_f64(), Some(10.0));
        assert_eq!(mutations[0].payload["close"].as_f64(), Some(10.0));

        let _ = std::fs::remove_file(&state_path);
    }

    #[test]
    fn decodes_uniswap_v2_swap_payload() {
        // V2-style swap shape:
        // amount0In=250, amount1In=0, amount0Out=0, amount1Out=1000
        let data = swap_data(250, 0, 0, 1000);
        let (price, volume_base, volume_quote) =
            decode_swap_price_and_volume(&data, 0, 0).expect("decode v2 swap");

        assert_eq!(volume_base, 250.0);
        assert_eq!(volume_quote, 1000.0);
        assert_eq!(price, 4.0);
    }
}
