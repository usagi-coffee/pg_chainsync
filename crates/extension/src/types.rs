use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use solana_account_decoder_client_types::UiDataSliceConfig;
use solana_client::rpc_filter::RpcFilterType;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::OnceCell;
use tokio::sync::oneshot;

use crate::evm::*;
use crate::svm::*;

#[derive(Clone, PartialEq)]
#[repr(u8)]
pub enum Signal {
    Unknown = 0,
    RestartBlocks = 1,
    RestartLogs = 2,
}

pub enum Message {
    Handlers(oneshot::Sender<Vec<HandlerRuntime>>),

    EvmBlock(EvmBlock, Arc<HandlerRuntime>),
    EvmLog(EvmLog, Arc<HandlerRuntime>),

    SvmBlock(SvmBlock, Arc<HandlerRuntime>),
    SvmLog(SvmLog, Arc<HandlerRuntime>),

    // Utility messages
    Shutdown,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HandlerRuntime {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub options: HandlerOptions,

    #[serde(skip_serializing, skip_deserializing)]
    pub evm: OnceCell<EvmPubSub>,

    #[serde(skip_serializing, skip_deserializing)]
    pub svm_ws: OnceCell<Arc<SvmPubSub>>,

    #[serde(skip_serializing, skip_deserializing)]
    pub svm_rpc: OnceCell<Arc<SvmRpc>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EvmOptions {
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub address: Option<String>,
    pub event: Option<String>,
    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,

    /// If defined it will split the rpc calls by the value, use when rpc limits number of blocks per call
    pub blocktick: Option<i64>,
    /// If defined it awaits for block before calling the handler
    pub await_block: Option<bool>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SvmOptions {
    // Filters instructions by the specific discriminators
    pub instruction_discriminators: Option<Vec<u8>>,
    pub accounts_filters: Option<Vec<RpcFilterType>>,
    pub accounts_data_slice: Option<UiDataSliceConfig>,

    pub from_slot: Option<u64>,
    pub to_slot: Option<u64>,
    pub mentions: Option<Vec<Arc<str>>>,

    #[serde(default, with = "custom_pubkey")]
    pub program: Option<Pubkey>,

    pub before: Option<Vec<Arc<str>>>,
    pub until: Option<Vec<Option<Arc<str>>>>,

    pub transaction_details: Option<SvmTransactionDetails>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HandlerOptions {
    /// RPC url to use for this handler
    pub rpc: Option<String>,
    /// Websocket ws url to use for this handler
    pub ws: Option<String>,

    /// Lookup query id -> SQL path (relative to handler dir)
    pub lookup_queries: Option<BTreeMap<String, String>>,
    /// Mutation query id -> SQL path (relative to handler dir)
    pub mutation_queries: Option<BTreeMap<String, String>>,
    /// Query ids to prefetch before first module call
    pub prelookups: Option<Vec<String>>,
    /// Absolute plugin path loaded from config scanner
    pub plugin_path: Option<String>,
    /// Content hash for handler module .so (embedded config included in binary).
    pub content_hash: Option<String>,
    /// Durable state file path for this handler
    pub state_path: Option<String>,
    /// Per-handler log file path
    pub log_path: Option<String>,

    pub evm: Option<EvmOptions>,
    pub svm: Option<SvmOptions>,
}

impl HandlerOptions {
    pub fn is_block_handler(&self) -> bool {
        if let Some(options) = &self.evm {
            return options.address.is_none()
                && options.event.is_none()
                && options.topic0.is_none()
                && options.topic1.is_none()
                && options.topic2.is_none()
                && options.topic3.is_none();
        } else if let Some(options) = &self.svm {
            return options.mentions.is_none();
        }

        false
    }

    pub fn is_log_handler(&self) -> bool {
        if let Some(options) = &self.evm {
            return options.event.is_some()
                || options.topic0.is_some()
                || options.topic1.is_some()
                || options.topic2.is_some()
                || options.topic3.is_some()
                || options.address.is_some();
        } else if let Some(options) = &self.svm {
            return options.mentions.is_some();
        }

        false
    }

}

impl From<u8> for Signal {
    fn from(orig: u8) -> Self {
        match orig {
            1 => return Signal::RestartBlocks,
            2 => return Signal::RestartLogs,
            _ => return Signal::Unknown,
        };
    }
}

pub trait HandlerRouting {
    fn svm_handlers(&self) -> Vec<HandlerRuntime>;
    fn evm_handlers(&self) -> Vec<HandlerRuntime>;

    fn block_handlers(&self) -> Vec<HandlerRuntime>;
    fn log_handlers(&self) -> Vec<HandlerRuntime>;
}

impl HandlerRouting for Vec<HandlerRuntime> {
    fn evm_handlers(&self) -> Vec<HandlerRuntime> {
        self.iter()
            .filter(|handler| matches!(handler.options.evm, Some(_)))
            .cloned()
            .collect()
    }

    fn svm_handlers(&self) -> Vec<HandlerRuntime> {
        self.iter()
            .filter(|handler| matches!(handler.options.svm, Some(_)))
            .cloned()
            .collect()
    }

    fn block_handlers(&self) -> Vec<HandlerRuntime> {
        self.iter()
            .filter(|handler| handler.options.is_block_handler())
            .cloned()
            .collect()
    }

    fn log_handlers(&self) -> Vec<HandlerRuntime> {
        self.iter()
            .filter(|handler| handler.options.is_log_handler())
            .cloned()
            .collect()
    }
}

use serde::{Deserializer, Serializer};

mod custom_pubkey {
    use super::*;
    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<Pubkey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let option: Option<String> = Option::deserialize(deserializer)?;
        match option {
            Some(s) => {
                let pubkey =
                    Pubkey::from_str(&s).map_err(serde::de::Error::custom)?;
                Ok(Some(pubkey))
            }
            None => Ok(None),
        }
    }

    pub fn serialize<S>(
        value: &Option<Pubkey>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(pubkey) => serializer.serialize_str(&pubkey.to_string()),
            None => serializer.serialize_none(),
        }
    }
}
