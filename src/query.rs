use pgx::prelude::*;
use pgx::{IntoDatum, PgOid};

use ethers::abi::AbiEncode;
use ethers::prelude::{Chain, Log};

use crate::types::*;

pub const BLOCK_COMPOSITE_TYPE: &str = "chainsync.Block";
pub const LOG_COMPOSITE_TYPE: &str = "chainsync.Log";

pub trait PgHandler {
    fn call_handler(
        &self,
        chain: &Chain,
        callback: &String,
    ) -> Result<(), pgx::spi::Error>;
}

impl Job {
    pub fn register(
        job_type: JobKind,
        chain_id: i64,
        provider_url: &str,
        callback: &str,
        options: pgx::JsonB,
    ) -> bool {
        Spi::run_with_args(
            include_str!("../sql/insert_job.sql"),
            Some(vec![
                (
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    job_type.to_string().into_datum(),
                ),
                (
                    PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                    chain_id.into_datum(),
                ),
                (
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    provider_url.into_datum(),
                ),
                (
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    callback.into_datum(),
                ),
                (
                    PgOid::BuiltIn(PgBuiltInOids::JSONBOID),
                    options.into_datum(),
                ),
            ]),
        )
        .is_ok()
    }

    pub fn update(&mut self, status: &String) -> bool {
        if let Ok(_) = Spi::run_with_args(
            "UPDATE chainsync.jobs SET status = $1 WHERE job_id = $2",
            Some(vec![
                (PgOid::BuiltIn(PgBuiltInOids::TEXTOID), status.into_datum()),
                (PgOid::BuiltIn(PgBuiltInOids::INT8OID), self.id.into_datum()),
            ]),
        ) {
            self.status = status.to_owned();
            return true;
        }

        return false;
    }

    pub fn query_all() -> Result<Vec<Job>, pgx::spi::Error> {
        Spi::connect(|client| {
            let mut table =
                client.select("SELECT * FROM chainsync.jobs", None, None)?;

            let mut jobs: Vec<Job> = Vec::new();
            while table.next().is_some() {
                let kind = table.get_by_name::<String, &'static str>("kind");

                if kind.is_err() {
                    continue;
                }

                let options = table
                    .get_by_name::<pgx::JsonB, &'static str>("options")
                    .unwrap()
                    .unwrap();

                jobs.push(Job {
                    id: table
                        .get_by_name::<i64, &'static str>("id")
                        .unwrap()
                        .unwrap(),
                    kind: kind.unwrap().unwrap().parse().unwrap(),
                    chain: Chain::try_from(
                        table
                            .get_by_name::<i64, &'static str>("chain_id")
                            .unwrap()
                            .unwrap() as u64,
                    )
                    .unwrap(),
                    provider_url: table
                        .get_by_name::<String, &'static str>("provider_url")
                        .unwrap()
                        .unwrap(),
                    callback: table
                        .get_by_name::<String, &'static str>("callback")
                        .unwrap()
                        .unwrap(),
                    status: table
                        .get_by_name::<String, &'static str>("status")
                        .unwrap()
                        .unwrap(),
                    options: serde_json::from_value(options.0).unwrap_or(None),
                    ws: None,
                });
            }

            Ok(jobs)
        })
    }
}

impl PgHandler for Block {
    fn call_handler(
        &self,
        chain: &Chain,
        callback: &String,
    ) -> Result<(), pgx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(BLOCK_COMPOSITE_TYPE).unwrap();

        data.set_by_name("chain", *chain as i64)?;
        data.set_by_name("hash", self.hash.unwrap_or_default().encode_hex())?;
        data.set_by_name("number", self.number.unwrap().as_u64() as i64)?;
        data.set_by_name(
            "timestamp",
            pgx::TimestampWithTimeZone::try_from(
                self.timestamp.as_u64() as i64
            ),
        )?;
        data.set_by_name(
            "author",
            self.author.unwrap_or_default().encode_hex(),
        )?;
        data.set_by_name("state_root", self.state_root.encode_hex())?;
        data.set_by_name("parent_hash", self.parent_hash.encode_hex())?;
        data.set_by_name("uncles_hash", self.uncles_hash.encode_hex())?;
        data.set_by_name(
            "transactions_root",
            self.transactions_root.encode_hex(),
        )?;

        data.set_by_name("receipts_root", self.receipts_root.encode_hex())?;
        data.set_by_name("gas_used", self.gas_used.as_u64() as i64)?;
        data.set_by_name("gas_limit", self.gas_limit.as_u64() as i64)?;
        data.set_by_name("timestamp", self.timestamp.as_u64() as i64)?;
        data.set_by_name("difficulty", self.difficulty.as_u64() as i64)?;
        data.set_by_name(
            "total_difficulty",
            self.total_difficulty.unwrap_or_default().as_u64() as i64,
        )?;
        data.set_by_name("size", self.size.unwrap().as_u64() as i64)?;

        Spi::run_with_args(
            format!("SELECT {}($1)", callback).as_str(),
            Some(vec![(
                PgOid::Custom(data.composite_type_oid().unwrap()),
                data.into_datum(),
            )]),
        )
    }
}

impl PgHandler for Log {
    fn call_handler(
        &self,
        chain: &Chain,
        callback: &String,
    ) -> Result<(), pgx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(LOG_COMPOSITE_TYPE).unwrap();

        data.set_by_name("chain", *chain as i64)?;
        data.set_by_name("removed", self.removed.unwrap())?;
        data.set_by_name("log_index", self.log_index.unwrap().as_u64() as i64)?;
        data.set_by_name(
            "transaction_hash",
            self.transaction_hash.unwrap().encode_hex(),
        )?;
        data.set_by_name(
            "transaction_index",
            self.transaction_index.unwrap().as_u64() as i64,
        )?;
        data.set_by_name(
            "transaction_log_index",
            self.transaction_log_index.unwrap_or_default().as_u64() as i64,
        )?;
        data.set_by_name("block_hash", self.block_hash.unwrap().encode_hex())?;
        data.set_by_name(
            "block_number",
            self.block_number.unwrap().as_u64() as i64,
        )?;
        data.set_by_name("address", self.address.encode_hex())?;
        data.set_by_name("data", self.data.to_string())?;
        data.set_by_name(
            "topics",
            self.topics
                .iter()
                .map(|&topic| topic.encode_hex())
                .collect::<Vec<String>>(),
        )?;

        Spi::run_with_args(
            format!("SELECT {}($1)", callback).as_str(),
            Some(vec![(
                PgOid::Custom(data.composite_type_oid().unwrap()),
                data.into_datum(),
            )]),
        )
    }
}
