use num_bigint::BigUint;
use pg_chainsync_sdk::{
    export_plugin, plugin_log, EvmIngressOverrides, IngressOverrides,
    ModuleResponse, Mutation, PluginHandler, SetupResponse,
};
use serde::Deserialize;

const TRANSFER_TOPIC0: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Debug, Deserialize)]
struct InputEvent {
    event: String,
    block_number: Option<u64>,
    transaction_hash: Option<String>,
    log_index: Option<u64>,
    address: Option<String>,
    topics: Option<Vec<String>>,
    data: Option<String>,
    ingest_unix: Option<u64>,
}

struct Erc20TransferHandler;

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x").unwrap_or(s)
}

fn hex_to_bytes(input: &str) -> Result<Vec<u8>, String> {
    let hex = strip_0x(input);
    if hex.len() % 2 != 0 {
        return Err("hex has odd length".into());
    }

    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let hi = (bytes[i] as char)
            .to_digit(16)
            .ok_or_else(|| "invalid hex".to_string())?;
        let lo = (bytes[i + 1] as char)
            .to_digit(16)
            .ok_or_else(|| "invalid hex".to_string())?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }

    Ok(out)
}

fn topic_to_address(topic: &str) -> Result<String, String> {
    let hex = strip_0x(topic);
    if hex.len() != 64 {
        return Err("topic must be 32 bytes".into());
    }
    Ok(format!("0x{}", &hex[24..64]).to_lowercase())
}

fn decode_amount_decimal(data: &str) -> Result<String, String> {
    let bytes = hex_to_bytes(data)?;
    if bytes.len() < 32 {
        return Err("transfer data is shorter than 32 bytes".into());
    }
    let amount = BigUint::from_bytes_be(&bytes[0..32]);
    Ok(amount.to_str_radix(10))
}

fn setup_response(input: &serde_json::Value) -> SetupResponse {
    let last_block = input
        .get("prefetched")
        .and_then(|v| v.get("recovery"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|v| v.get("last_block"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    if last_block <= 0 {
        plugin_log!(
            "setup: recovery last_block missing/zero; no ingress override"
        );
        return SetupResponse::default();
    }

    plugin_log!(
        "setup: applying recovery override evm.from_block={}",
        last_block
    );
    SetupResponse {
        ingress_overrides: Some(IngressOverrides {
            ws: None,
            rpc: None,
            evm: Some(EvmIngressOverrides {
                from_block: Some(last_block),
                to_block: None,
            }),
            svm: None,
        }),
    }
}

fn process_event(input: serde_json::Value) -> Result<ModuleResponse, String> {
    let parsed: InputEvent =
        serde_json::from_value(input).map_err(|e| e.to_string())?;

    if parsed.event != "evm_log" {
        plugin_log!("ignoring non-evm_log event");
        return Ok(ModuleResponse::Ignore);
    }

    let topics = match parsed.topics {
        Some(t) if t.len() >= 3 => t,
        _ => {
            plugin_log!("ignoring log with missing/short topics");
            return Ok(ModuleResponse::Ignore);
        }
    };

    if topics[0].to_lowercase() != TRANSFER_TOPIC0 {
        plugin_log!("ignoring log with non-transfer topic0");
        return Ok(ModuleResponse::Ignore);
    }

    let contract = parsed
        .address
        .ok_or_else(|| "missing contract address".to_string())?
        .to_lowercase();
    let from_address = topic_to_address(&topics[1])?;
    let to_address = topic_to_address(&topics[2])?;
    let amount_raw = decode_amount_decimal(
        parsed
            .data
            .as_deref()
            .ok_or_else(|| "missing log data".to_string())?,
    )?;

    let tx_hash = parsed
        .transaction_hash
        .ok_or_else(|| "missing transaction_hash".to_string())?;
    let log_index = parsed
        .log_index
        .ok_or_else(|| "missing log_index".to_string())?;
    let block_number = parsed
        .block_number
        .ok_or_else(|| "missing block_number".to_string())?;

    let payload = serde_json::json!({
        "contract": contract,
        "from": from_address,
        "to": to_address,
        "amount_raw": amount_raw,
        "tx_hash": tx_hash,
        "log_index": log_index,
        "block_number": block_number,
        "ingest_unix": parsed.ingest_unix,
    });

    plugin_log!(
        "tx={} log_index={} block={} contract={} from={} to={} amount_raw={}",
        tx_hash,
        log_index,
        block_number,
        contract,
        from_address,
        to_address,
        amount_raw
    );

    Ok(ModuleResponse::Done {
        mutations: vec![Mutation {
            id: "upsert_transfer".to_string(),
            payload,
        }],
    })
}

impl PluginHandler for Erc20TransferHandler {
    fn setup(input: serde_json::Value) -> Result<SetupResponse, String> {
        Ok(setup_response(&input))
    }

    fn handle_event(
        input: serde_json::Value,
    ) -> Result<ModuleResponse, String> {
        match process_event(input) {
            Ok(response) => Ok(response),
            Err(error) => {
                plugin_log!("handler error: {}", error);
                Err(error)
            }
        }
    }
}

export_plugin!(Erc20TransferHandler, "erc20_transfer_handler", "0.1.0");

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn topic_with_address(addr: &str) -> String {
        let compact = addr.trim_start_matches("0x").to_lowercase();
        format!("0x{:0>64}", compact)
    }

    fn amount_word_hex(value: u128) -> String {
        format!("{value:064x}")
    }

    fn base_event() -> serde_json::Value {
        json!({
            "event": "evm_log",
            "block_number": 12345u64,
            "transaction_hash": "0xtxhash",
            "log_index": 7u64,
            "address": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "topics": [
                TRANSFER_TOPIC0,
                topic_with_address("0x1111111111111111111111111111111111111111"),
                topic_with_address("0x2222222222222222222222222222222222222222")
            ],
            "data": amount_word_hex(42),
            "ingest_unix": 1700000000u64
        })
    }

    #[test]
    fn ignores_non_evm_log() {
        let mut event = base_event();
        event["event"] = json!("svm_log");
        let out = process_event(event).expect("process event");
        assert!(matches!(out, ModuleResponse::Ignore));
    }

    #[test]
    fn ignores_non_transfer_topic0() {
        let mut event = base_event();
        event["topics"][0] = json!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let out = process_event(event).expect("process event");
        assert!(matches!(out, ModuleResponse::Ignore));
    }

    #[test]
    fn emits_upsert_mutation_with_decoded_transfer_fields() {
        let out = process_event(base_event()).expect("process event");
        let ModuleResponse::Done { mutations } = out else {
            panic!("expected done");
        };

        assert_eq!(mutations.len(), 1);
        let mutation = &mutations[0];
        assert_eq!(mutation.id, "upsert_transfer");
        assert_eq!(
            mutation.payload["contract"].as_str(),
            Some("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        );
        assert_eq!(
            mutation.payload["from"].as_str(),
            Some("0x1111111111111111111111111111111111111111")
        );
        assert_eq!(
            mutation.payload["to"].as_str(),
            Some("0x2222222222222222222222222222222222222222")
        );
        assert_eq!(mutation.payload["amount_raw"].as_str(), Some("42"));
        assert_eq!(mutation.payload["tx_hash"].as_str(), Some("0xtxhash"));
        assert_eq!(mutation.payload["log_index"].as_u64(), Some(7));
        assert_eq!(mutation.payload["block_number"].as_u64(), Some(12345));
        assert_eq!(mutation.payload["ingest_unix"].as_u64(), Some(1700000000));
    }

    #[test]
    fn errors_when_required_fields_missing() {
        let mut event = base_event();
        event["data"] = serde_json::Value::Null;
        let err = process_event(event).expect_err("expected error");
        assert_eq!(err, "missing log data");
    }

    #[test]
    fn setup_returns_recovery_from_block_override() {
        let input = json!({
            "prefetched": {
                "recovery": [
                    { "last_block": 1000 }
                ]
            }
        });
        let setup = setup_response(&input);
        let from_block = setup
            .ingress_overrides
            .and_then(|o| o.evm)
            .and_then(|e| e.from_block);
        assert_eq!(from_block, Some(1001));
    }
}
