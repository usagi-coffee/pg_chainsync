use num_bigint::BigUint;
use pg_chainsync_sdk::{
    export_plugin, ModuleResponse, Mutation, PluginHandler,
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

fn process_event(input: serde_json::Value) -> Result<ModuleResponse, String> {
    let parsed: InputEvent =
        serde_json::from_value(input).map_err(|e| e.to_string())?;

    if parsed.event != "evm_log" {
        return Ok(ModuleResponse::Ignore);
    }

    let topics = match parsed.topics {
        Some(t) if t.len() >= 3 => t,
        _ => return Ok(ModuleResponse::Ignore),
    };

    if topics[0].to_lowercase() != TRANSFER_TOPIC0 {
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

    Ok(ModuleResponse::Done {
        mutations: vec![Mutation {
            id: "upsert_transfer".to_string(),
            payload,
        }],
    })
}

impl PluginHandler for Erc20TransferHandler {
    fn handle_event(input: serde_json::Value) -> Result<ModuleResponse, String> {
        process_event(input)
    }
}

export_plugin!(
    Erc20TransferHandler,
    "erc20_transfer_handler",
    "0.1.0",
    include_str!("../handler.toml")
);
