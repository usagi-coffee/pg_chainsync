use std::str;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use tokio::sync::oneshot;
use tokio::sync::OnceCell;

use alloy_chains::Chain;

pub type EvmPubSub =
    alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend>;
pub type EvmPubSubError =
    alloy::transports::RpcError<alloy::transports::TransportErrorKind>;

pub type SolanaPubSub = solana_client::nonblocking::pubsub_client::PubsubClient;
pub type SolanaPubSubError = solana_client::pubsub_client::PubsubClientError;

pub const JOB_COMPOSITE_TYPE: &str = "chainsync.Job";

pub type Callback = String;

pub enum Block {
    EvmBlock(alloy::rpc::types::Header),
}

pub enum Log {
    EvmLog(alloy::rpc::types::Log),
}

#[derive(Clone, PartialEq)]
#[repr(u8)]
pub enum Signal {
    Unknown = 0,
    RestartBlocks = 1,
    RestartLogs = 2,
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

    EvmBlock(Block, Arc<Job>),
    EvmLog(Log, Arc<Job>),

    // Tasks
    TaskSuccess(Arc<Job>),
    TaskFailure(Arc<Job>),

    // Utility messages
    CheckBlock(u64, oneshot::Sender<bool>, Arc<Job>),
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
    pub sol: OnceCell<Arc<SolanaPubSub>>,
}

impl Job {
    pub async fn connect_evm(
        &self,
    ) -> anyhow::Result<&EvmPubSub, EvmPubSubError> {
        let url = &self.options.ws;
        self.evm
            .get_or_try_init(|| async {
                let ws = alloy::providers::WsConnect::new(url);
                alloy::providers::ProviderBuilder::new().on_ws(ws).await
            })
            .await
    }

    pub async fn connect_sol(
        &self,
    ) -> anyhow::Result<&Arc<SolanaPubSub>, SolanaPubSubError> {
        let url = &self.options.ws;
        self.sol
            .get_or_try_init(|| async {
                let r = SolanaPubSub::new(&url);
                r.await.map(Arc::new)
            })
            .await
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobOptions {
    /// Chain id
    pub chain: Chain,
    /// Websocket ws url to use for this job
    pub ws: String,
    /// If defined it will start during immediately after database startup
    pub preload: Option<bool>,
    /// If defined it will split the rpc calls by the value, use when rpc limits number of blocks per call
    pub blocktick: Option<i64>,

    // Range
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,

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

    // Filter options
    pub address: Option<String>,
    pub event: Option<String>,
    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
}

impl JobOptions {
    pub fn is_block_job(&self) -> bool {
        matches!(self.log_handler, None)
            && matches!(self.block_handler, Some(_))
    }

    pub fn is_event_job(&self) -> bool {
        matches!(self.log_handler, Some(_))
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
    fn block_jobs(&self) -> Vec<Job>;
    fn log_jobs(&self) -> Vec<Job>;
    fn preload_jobs(&self) -> Vec<Job>;
    fn tasks(&self) -> Vec<Job>;
}

impl JobsUtils for Vec<Job> {
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
                job.options.is_event_job()
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
