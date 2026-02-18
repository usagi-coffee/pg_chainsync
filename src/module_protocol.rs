use anyhow::{Result, bail};
use pgrx::JsonB;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModuleResponse {
    Ignore,
    Error { message: String },
    Done { mutations: Vec<Mutation> },
}

#[derive(Debug, Deserialize)]
pub struct Mutation {
    pub id: String,
    pub payload: serde_json::Value,
}

pub fn decode_response(bytes: &[u8]) -> Result<ModuleResponse> {
    if bytes.is_empty() {
        return Ok(ModuleResponse::Ignore);
    }
    let parsed: ModuleResponse = serde_json::from_slice(bytes)?;
    Ok(parsed)
}

pub fn payload_to_jsonb(value: serde_json::Value) -> Result<JsonB> {
    if !value.is_object() {
        bail!("mutation payload must be a JSON object");
    }
    Ok(JsonB(value))
}
