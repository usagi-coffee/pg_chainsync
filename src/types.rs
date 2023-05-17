use std::fmt;
use std::hash::Hash;
use std::str;

use serde::Deserialize;

use ethers::prelude::*;
use ethers::types::{Chain, H256};

use tokio::sync::oneshot;

pub type Callback = String;
pub type Block = ethers::types::Block<H256>;
pub type Log = ethers::types::Log;

pub enum Message {
    // Job messages
    Block(Chain, Block, Callback),
    Event(Chain, Log, Callback),

    // Utility messages
    CheckBlock(Chain, u64, Callback, oneshot::Sender<bool>),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum JobKind {
    Blocks,
    Events,
}

pub struct Job {
    pub id: i64,
    pub kind: JobKind,
    pub chain: Chain,
    pub provider_url: String,
    pub status: String,
    pub callback: String,
    pub options: Option<JobOptions>,

    pub ws: Option<Provider<Ws>>,
}

#[derive(Deserialize)]
pub struct JobOptions {
    /// Generic
    // Nothing

    /// Block job
    // Nothing

    /// Event job
    // If defined it awaits for block before calling the handler
    pub await_block: Option<AwaitBlock>,

    // Filter options
    pub address: Option<String>,
    pub event: Option<String>,
    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
}

#[derive(Deserialize)]
pub struct AwaitBlock {
    pub check_block: Option<String>,
    pub block_handler: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParseJobError(&'static str);

impl std::str::FromStr for JobKind {
    type Err = ParseJobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "Blocks" => JobKind::Blocks,
            "Events" => JobKind::Events,
            _ => return Err(ParseJobError("Failed to parse job type")),
        })
    }
}

impl fmt::Display for JobKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JobKind::Blocks => write!(f, "Blocks"),
            JobKind::Events => write!(f, "Events"),
        }
    }
}
