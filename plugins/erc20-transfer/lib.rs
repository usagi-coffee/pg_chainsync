use alloy_primitives::{Address, B256, U256};
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
    plugin: Option<PluginOptions>,
    block_number: Option<u64>,
    transaction_hash: Option<String>,
    log_index: Option<u64>,
    address: Option<String>,
    topics: Option<Vec<String>>,
    data: Option<String>,
    ingest_unix: Option<u64>,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct PluginOptions {
    mutation_id: Option<String>,
    transfer_topic0: Option<String>,
    recovery_lookup_id: Option<String>,
}

struct Erc20TransferHandler;

fn topic_to_address(topic: &str) -> Result<String, String> {
    let word: B256 = topic
        .parse()
        .map_err(|_| "topic must be valid 32-byte hex".to_string())?;
    let address = Address::from_slice(&word.as_slice()[12..32]);
    Ok(format!("{:#x}", address))
}

fn decode_amount_decimal(data: &str) -> Result<String, String> {
    let word: B256 = data
        .parse()
        .map_err(|_| "transfer data must be valid 32-byte hex".to_string())?;
    let amount = U256::from_be_slice(word.as_slice());
    Ok(amount.to_string())
}

fn setup_response(input: &serde_json::Value) -> SetupResponse {
    let plugin: PluginOptions = input
        .get("plugin")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .unwrap_or(None)
        .unwrap_or_default();
    let recovery_lookup_id =
        plugin.recovery_lookup_id.as_deref().unwrap_or("recovery");
    let last_block = input
        .get("prefetched")
        .and_then(|v| v.get(recovery_lookup_id))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|v| v.get("last_block"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    if last_block <= 0 {
        plugin_log!(
            "setup: lookup={} last_block missing/zero; no ingress override",
            recovery_lookup_id
        );
        return SetupResponse::default();
    }

    plugin_log!(
        "setup: lookup={} applying recovery override evm.from_block={}",
        recovery_lookup_id,
        last_block,
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
    let plugin = parsed.plugin.clone().unwrap_or_default();
    let transfer_topic0 =
        plugin.transfer_topic0.as_deref().unwrap_or(TRANSFER_TOPIC0);
    let mutation_id =
        plugin.mutation_id.as_deref().unwrap_or("upsert_transfer");

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

    let transfer_topic: B256 = transfer_topic0
        .parse()
        .map_err(|_| "invalid transfer topic constant".to_string())?;
    let topic0: B256 = topics[0]
        .parse()
        .map_err(|_| "topic0 must be valid 32-byte hex".to_string())?;
    if topic0 != transfer_topic {
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
            id: mutation_id.to_string(),
            payload,
        }],
    })
}

impl PluginHandler for Erc20TransferHandler {
    fn setup(input: serde_json::Value) -> Result<SetupResponse, String> {
        Ok(setup_response(&input))
    }

    fn handle(input: serde_json::Value) -> Result<ModuleResponse, String> {
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
        assert_eq!(from_block, Some(1000));
    }
}
