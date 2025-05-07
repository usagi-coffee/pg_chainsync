pub mod evm;
pub mod svm;

use std::str;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use solana_sdk::pubkey::Pubkey;
use tokio::sync::oneshot;
use tokio::sync::OnceCell;

use crate::types::evm::*;
use crate::types::svm::*;

pub const JOB_COMPOSITE_TYPE: &str = "chainsync.Job";

pub type Callback = String;

#[derive(Clone, PartialEq)]
#[repr(u8)]
pub enum Signal {
    Unknown = 0,
    RestartBlocks = 1,
    RestartLogs = 2,
    RestartTransactions = 3,
}

#[derive(Clone, PartialEq)]
#[repr(u8)]
pub enum JobStatus {
    Stopped = 0,
    Running = 1,
}

pub enum Message {
    Job(i64, oneshot::Sender<Option<Job>>),
    Jobs(oneshot::Sender<Vec<Job>>),
    UpdateJob(JobStatus, Arc<Job>),

    EvmBlock(EvmBlock, Arc<Job>),
    EvmLog(EvmLog, Arc<Job>),

    SvmBlock(SvmBlock, Arc<Job>),
    SvmLog(SvmLog, Arc<Job>),
    SvmTransaction(SvmTransaction, Arc<Job>),

    // Tasks
    TaskSuccess(Arc<Job>),
    TaskFailure(Arc<Job>),

    // Utility messages
    CheckBlock(u64, oneshot::Sender<bool>, Arc<Job>),
    Shutdown,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub options: JobOptions,

    #[serde(skip_serializing, skip_deserializing)]
    pub evm: OnceCell<EvmPubSub>,

    #[serde(skip_serializing, skip_deserializing)]
    pub svm_ws: OnceCell<Arc<SvmPubSub>>,

    #[serde(skip_serializing, skip_deserializing)]
    pub svm_rpc: OnceCell<Arc<SvmRpc>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobOptions {
    /// RPC url to use for this job
    pub rpc: Option<String>,
    /// Websocket ws url to use for this job
    pub ws: Option<String>,

    /// If defined it will start during immediately after database startup
    pub preload: Option<bool>,
    /// If defined it will split the rpc calls by the value, use when rpc limits number of blocks per call
    pub blocktick: Option<i64>,

    // Tasks
    /// This modifies the job to not restart
    pub oneshot: Option<bool>,
    /// If defined will start the job on the given cron expression
    pub cron: Option<String>,

    // Handlers
    pub success_handler: Option<String>,
    pub failure_handler: Option<String>,

    /// Block job
    pub block_handler: Option<String>,
    // TODO: hashes vs full blocks?

    // Log job
    /// Function to call when handling events
    pub log_handler: Option<String>,
    /// If defined it awaits for block before calling the handler
    pub await_block: Option<bool>,
    /// If defined it awaits for block before calling the handler
    pub block_check_handler: Option<String>,

    // Transaction job
    pub transaction_handler: Option<String>,
    pub instruction_handler: Option<String>,

    // EVM: Filter options
    pub evm: Option<bool>,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub address: Option<String>,
    pub event: Option<String>,
    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,

    // SVM: Filter options
    pub svm: Option<bool>,
    pub from_slot: Option<u64>,
    pub to_slot: Option<u64>,
    pub mentions: Option<Vec<String>>,
    #[serde(default, with = "custom_pubkey")]
    pub program: Option<Pubkey>,
    pub before: Option<String>,
    pub until: Option<String>,

    // SVM: Block Config
    pub transaction_details: Option<SvmTransactionDetails>,
}

impl JobOptions {
    pub fn is_block_job(&self) -> bool {
        matches!(self.log_handler, None)
            && matches!(self.block_handler, Some(_))
    }

    pub fn is_log_job(&self) -> bool {
        matches!(self.log_handler, Some(_))
    }

    pub fn is_transaction_job(&self) -> bool {
        matches!(self.transaction_handler, Some(_))
            || matches!(self.instruction_handler, Some(_))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ParseJobError(&'static str);

impl From<u8> for Signal {
    fn from(orig: u8) -> Self {
        match orig {
            1 => return Signal::RestartBlocks,
            2 => return Signal::RestartLogs,
            3 => return Signal::RestartTransactions,
            _ => return Signal::Unknown,
        };
    }
}

impl Into<String> for JobStatus {
    fn into(self) -> String {
        match self {
            JobStatus::Stopped => "STOPPED".to_string(),
            JobStatus::Running => "RUNNING".to_string(),
        }
    }
}

pub trait JobsUtils {
    fn svm_jobs(&self) -> Vec<Job>;
    fn evm_jobs(&self) -> Vec<Job>;

    fn block_jobs(&self) -> Vec<Job>;
    fn log_jobs(&self) -> Vec<Job>;
    fn transaction_jobs(&self) -> Vec<Job>;
    fn preload_jobs(&self) -> Vec<Job>;
    fn tasks(&self) -> Vec<Job>;
}

impl JobsUtils for Vec<Job> {
    fn evm_jobs(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| matches!(job.options.evm, Some(true)))
            .cloned()
            .collect()
    }

    fn svm_jobs(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| matches!(job.options.svm, Some(true)))
            .cloned()
            .collect()
    }

    fn block_jobs(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| {
                job.options.is_block_job()
                    && matches!(job.options.oneshot, None | Some(false))
                    && matches!(job.options.cron, None)
                    && matches!(job.options.preload, None | Some(false))
            })
            .cloned()
            .collect()
    }

    fn log_jobs(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| {
                job.options.is_log_job()
                    && matches!(job.options.oneshot, None | Some(false))
                    && matches!(job.options.cron, None)
                    && matches!(job.options.preload, None | Some(false))
            })
            .cloned()
            .collect()
    }

    fn transaction_jobs(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| {
                job.options.is_transaction_job()
                    && matches!(job.options.oneshot, None | Some(false))
                    && matches!(job.options.cron, None)
                    && matches!(job.options.preload, None | Some(false))
            })
            .cloned()
            .collect()
    }

    fn preload_jobs(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| matches!(job.options.preload, Some(true)))
            .cloned()
            .collect()
    }

    fn tasks(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| {
                matches!(job.options.oneshot, Some(true))
                    || job.options.cron.is_some()
            })
            .cloned()
            .collect()
    }
}

use serde::{Deserializer, Serializer};
use std::str::FromStr;

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
