use std::str;
use std::str::FromStr;
use std::sync::Arc;

use chrono::DateTime;
use pgrx::JsonB;
use serde::{Deserialize, Serialize};

use solana_sdk::pubkey::Pubkey;
use tokio::sync::oneshot;
use tokio::sync::OnceCell;

use chrono::Utc;
use cron::Schedule;

use crate::evm::*;
use crate::svm::*;

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
    Waiting = 1,
    Running = 2,
}

#[derive(Debug)]
pub enum PostgresArg {
    Void,
    Boolean(bool),
    Integer(i32),
    BigInt(i64),
    String(String),
    Json(JsonB),
}
pub type PostgresReturn = PostgresArg;
pub enum PostgresSender {
    Void(oneshot::Sender<()>),
    Integer(oneshot::Sender<i32>),
    BigInt(oneshot::Sender<i64>),
    String(oneshot::Sender<String>),
    Boolean(oneshot::Sender<bool>),
    Json(oneshot::Sender<JsonB>),
}

impl PostgresSender {
    pub fn send(self, value: PostgresReturn) -> bool {
        match (self, value) {
            (PostgresSender::Void(tx), PostgresReturn::Void) => {
                tx.send(()).is_ok()
            }
            (PostgresSender::Integer(tx), PostgresReturn::Integer(i)) => {
                tx.send(i).is_ok()
            }
            (PostgresSender::BigInt(tx), PostgresReturn::BigInt(i)) => {
                tx.send(i).is_ok()
            }
            (PostgresSender::String(tx), PostgresReturn::String(s)) => {
                tx.send(s).is_ok()
            }
            (PostgresSender::Boolean(tx), PostgresReturn::Boolean(b)) => {
                tx.send(b).is_ok()
            }
            (PostgresSender::Json(tx), PostgresReturn::Json(j)) => {
                tx.send(j).is_ok()
            }
            (_, _) => false,
        }
    }
}

pub enum Message {
    Job(i64, oneshot::Sender<Option<Job>>),
    Jobs(oneshot::Sender<Vec<Job>>),
    UpdateJob(i64, JobStatus),

    EvmBlock(EvmBlock, Arc<Job>),
    EvmLog(EvmLog, Arc<Job>),

    SvmBlock(SvmBlock, Arc<Job>),
    SvmLog(SvmLog, Arc<Job>),
    SvmTransaction(SvmTransaction, Arc<Job>),

    // Handlers
    Handler(Arc<str>, oneshot::Sender<bool>, Arc<Job>),
    ReturnHandler(Arc<str>, PostgresSender, Arc<Job>),
    ReturnHandlerWithArg(PostgresArg, Arc<str>, PostgresSender, Arc<Job>),
    // Utility messages
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
pub struct EvmOptions {
    pub block_handler: Option<Arc<str>>,
    pub log_handler: Option<Arc<str>>,

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
    /// If defined it will skip the log from processing
    pub block_skip_lookup: Option<Arc<str>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SvmOptions {
    pub block_handler: Option<Arc<str>>,
    /// Function to call when handling events
    pub log_handler: Option<Arc<str>>,
    // Transaction job
    pub transaction_handler: Option<Arc<str>>,
    // If defined it will skip the transaction from processing
    pub transaction_check_handler: Option<Arc<str>>,
    // Instruction job
    pub instruction_handler: Option<Arc<str>>,
    // Filters instructions by the specific discriminators
    pub instruction_discriminators: Option<Vec<u8>>,
    /// If defined it will fetch account owner from the database before inserting instruction/transaction
    pub account_owner_lookup: Option<Arc<str>>,
    /// If defined it will fetch account mint from the database before inserting instruction/transaction
    pub account_mint_lookup: Option<Arc<str>>,

    pub from_slot: Option<u64>,
    pub to_slot: Option<u64>,
    pub mentions: Option<Vec<String>>,

    #[serde(default, with = "custom_pubkey")]
    pub program: Option<Pubkey>,
    pub before: Option<String>,
    pub until: Option<String>,

    pub transaction_details: Option<SvmTransactionDetails>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobOptions {
    /// RPC url to use for this job
    pub rpc: Option<String>,
    /// Websocket ws url to use for this job
    pub ws: Option<String>,

    /// If defined it will start during immediately after database startup
    pub preload: Option<bool>,

    // Tasks
    /// This modifies the job to not restart
    pub oneshot: Option<bool>,
    /// If defined will start the job on the given cron expression
    pub cron: Option<String>,

    // Handlers
    pub setup_handler: Option<Arc<str>>,
    pub success_handler: Option<Arc<str>>,
    pub failure_handler: Option<Arc<str>>,

    pub evm: Option<EvmOptions>,
    pub svm: Option<SvmOptions>,
}

impl JobOptions {
    pub fn is_block_job(&self) -> bool {
        if let Some(options) = &self.evm {
            return options.block_handler.is_some()
                && options.log_handler.is_none();
        } else if let Some(options) = &self.svm {
            return options.block_handler.is_some()
                && options.log_handler.is_none()
                && options.transaction_handler.is_none();
        }

        false
    }

    pub fn is_log_job(&self) -> bool {
        if let Some(options) = &self.evm {
            return options.log_handler.is_some();
        } else if let Some(options) = &self.svm {
            return options.log_handler.is_some();
        }

        false
    }

    pub fn is_transaction_job(&self) -> bool {
        if let Some(options) = &self.svm {
            return options.log_handler.is_some()
                || options.transaction_handler.is_some();
        }

        false
    }

    pub fn next_cron(&self) -> Option<DateTime<Utc>> {
        if let Some(cron) = &self.cron {
            let schedule = Schedule::from_str(cron).ok()?;
            let now = Utc::now();
            let mut iter = schedule.after(&now);
            iter.next()
        } else {
            None
        }
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

impl From<JobStatus> for String {
    fn from(orig: JobStatus) -> Self {
        match orig {
            JobStatus::Stopped => "STOPPED",
            JobStatus::Waiting => "WAITING",
            JobStatus::Running => "RUNNING",
        }
        .into()
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
            .filter(|job| matches!(job.options.evm, Some(_)))
            .cloned()
            .collect()
    }

    fn svm_jobs(&self) -> Vec<Job> {
        self.iter()
            .filter(|job| matches!(job.options.svm, Some(_)))
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
                (job.options.is_log_job() || job.options.is_transaction_job())
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
