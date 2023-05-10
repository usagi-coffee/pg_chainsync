use std::fmt;
use std::hash::Hash;
use std::panic::UnwindSafe;
use std::str;

use serde::Deserialize;

use ethers::prelude::*;
use ethers::types::{Chain, H256};

pub enum Message {
    Block(ethers::types::Block<H256>),
    Event(ethers::types::Log, String),
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
pub struct JobOptions {
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
