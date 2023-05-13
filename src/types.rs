use std::fmt;
use std::hash::Hash;
use std::panic::UnwindSafe;
use std::str;

use serde::Deserialize;

use ethers::prelude::*;
use ethers::types::{Chain, H256};

use tokio::sync::oneshot;

type Callback = String;

pub enum Message {
    // Job messages
    Block(Chain, ethers::types::Block<H256>, Callback),
    Event(Chain, ethers::types::Log, Callback),
    // Utility messages
    CheckBlock(Chain, u64, Callback, oneshot::Sender<bool>),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum JobType {
    Blocks,
    Events,
}

#[derive(Debug, Clone)]
pub struct ParseJobError(&'static str);

impl std::str::FromStr for JobType {
    type Err = ParseJobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "Blocks" => JobType::Blocks,
            "Events" => JobType::Events,
            _ => return Err(ParseJobError("Failed to parse job type")),
        })
    }
}

impl fmt::Display for JobType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JobType::Blocks => write!(f, "Blocks"),
            JobType::Events => write!(f, "Events"),
        }
    }
}

#[derive(Deserialize)]
pub struct AwaitBlock {
    pub check_block: Option<String>,
    pub block_handler: Option<String>,
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
    pub from_block: Option<i32>,
    pub to_block: Option<i32>,
}

pub struct Job {
    pub job_type: JobType,
    pub chain: Chain,
    pub provider_url: String,
    pub callback: String,
    pub options: Option<JobOptions>,

    pub ws: Option<Provider<Ws>>,
}

impl UnwindSafe for Job {}
