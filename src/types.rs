use std::fmt;
use std::hash::Hash;
use std::panic::UnwindSafe;
use std::str;

use ethers::prelude::*;
use ethers::types::{Chain, H256};

pub enum Message {
    Block(ethers::types::Block<H256>),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum JobType {
    Blocks,
}

#[derive(Debug, Clone)]
pub struct ParseJobError(&'static str);

impl std::str::FromStr for JobType {
    type Err = ParseJobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "Blocks" => JobType::Blocks,
            _ => return Err(ParseJobError("Failed to parse job type")),
        })
    }
}

impl fmt::Display for JobType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JobType::Blocks => write!(f, "Blocks"),
        }
    }
}

pub struct Job {
    pub job_type: JobType,
    pub chain: Chain,
    pub provider_url: String,
    pub callback: String,

    pub ws: Option<Provider<Ws>>,
}

impl UnwindSafe for Job {}
