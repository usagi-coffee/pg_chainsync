use pgrx::prelude::*;
use pgrx::{IntoDatum, PgOid};

use tokio::sync::OnceCell;

use alloy::core::hex;

use crate::types::*;

pub const BLOCK_COMPOSITE_TYPE: &str = "chainsync.Block";
pub const LOG_COMPOSITE_TYPE: &str = "chainsync.Log";

impl Job {
    pub fn register(name: String, options: pgrx::JsonB) -> i64 {
        match Spi::get_one_with_args(
            "INSERT INTO chainsync.jobs (name, options) VALUES ($1, $2) RETURNING id",
            vec![
                (
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    name.into_datum(),
                ),
                (
                    PgOid::BuiltIn(PgBuiltInOids::JSONBOID),
                    options.into_datum(),
                ),
            ],
        ) {
            Ok(id) => id.unwrap(),
            Err(_) => -1,
        }
    }

    pub fn update(id: i64, status: JobStatus) -> Result<(), pgrx::spi::Error> {
        let status_text: String = status.into();

        Spi::run_with_args(
            "UPDATE chainsync.jobs SET status = $1 WHERE id = $2",
            Some(vec![
                (
                    PgOid::BuiltIn(PgBuiltInOids::TEXTOID),
                    status_text.clone().into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT8OID), id.into_datum()),
            ]),
        )?;

        Ok(())
    }

    pub fn query_all() -> Result<Vec<Job>, pgrx::spi::Error> {
        Spi::connect(|client| {
            let mut table =
                client.select("SELECT * FROM chainsync.jobs", None, None)?;

            let mut jobs: Vec<Job> = Vec::new();
            while table.next().is_some() {
                let options = table
                    .get_by_name::<pgrx::JsonB, &'static str>("options")
                    .unwrap()
                    .unwrap();

                jobs.push(Job {
                    id: table
                        .get_by_name::<i64, &'static str>("id")
                        .unwrap()
                        .unwrap(),
                    name: table
                        .get_by_name::<String, &'static str>("name")
                        .unwrap()
                        .unwrap(),
                    options: serde_json::from_value(options.0.clone())
                        .expect("Invalid options"),
                    status: table
                        .get_by_name::<String, &'static str>("status")
                        .unwrap()
                        .unwrap(),
                    evm: OnceCell::const_new(),
                });
            }

            Ok(jobs)
        })
    }

    pub fn handler(handler: &String, job: i64) -> Result<(), pgrx::spi::Error> {
        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $1))", handler).as_str(),
            Some(vec![
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )
    }
}

pub trait PgHandler {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<(), pgrx::spi::Error>;
}

impl PgHandler for alloy::rpc::types::Header {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<(), pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(BLOCK_COMPOSITE_TYPE).unwrap();

        data.set_by_name("number", pgrx::AnyNumeric::try_from(self.number))?;
        data.set_by_name("hash", hex::encode(self.hash))?;
        data.set_by_name("author", hex::encode(self.beneficiary))?;
        data.set_by_name(
            "difficulty",
            pgrx::AnyNumeric::try_from(self.difficulty.to_string().as_str()),
        )?;

        if let Some(total_difficulty) = self.total_difficulty {
            data.set_by_name(
                "total_difficulty",
                pgrx::AnyNumeric::try_from(
                    total_difficulty.to_string().as_str(),
                ),
            )?;
        }

        data.set_by_name("state_root", hex::encode(self.state_root))?;
        data.set_by_name("parent_hash", hex::encode(self.parent_hash))?;
        data.set_by_name("omners_hash", hex::encode(self.ommers_hash))?;
        data.set_by_name(
            "transactions_root",
            hex::encode(self.transactions_root),
        )?;
        data.set_by_name("receipts_root", hex::encode(self.receipts_root))?;
        data.set_by_name(
            "gas_used",
            pgrx::AnyNumeric::try_from(self.gas_used),
        )?;
        data.set_by_name(
            "gas_limit",
            pgrx::AnyNumeric::try_from(self.gas_limit),
        )?;

        if let Some(size) = self.size {
            data.set_by_name(
                "size",
                pgrx::AnyNumeric::try_from(size.to_string().as_str()),
            )?;
        }

        data.set_by_name(
            "timestamp",
            pgrx::AnyNumeric::try_from(self.timestamp),
        )?;

        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )
    }
}

impl PgHandler for alloy::rpc::types::Log {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<(), pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(LOG_COMPOSITE_TYPE).unwrap();

        data.set_by_name(
            "block_number",
            pgrx::AnyNumeric::try_from(
                self.block_number.unwrap().to_string().as_str(),
            ),
        )?;
        data.set_by_name("block_hash", hex::encode(self.block_hash.unwrap()))?;
        data.set_by_name(
            "transaction_hash",
            hex::encode(self.transaction_hash.unwrap()),
        )?;
        data.set_by_name(
            "transaction_index",
            self.transaction_index.unwrap() as i64,
        )?;
        data.set_by_name("log_index", self.log_index.unwrap() as i64)?;
        data.set_by_name("address", hex::encode(self.address()))?;
        data.set_by_name(
            "topics",
            self.topics()
                .iter()
                .map(|&topic| hex::encode(topic))
                .collect::<Vec<String>>(),
        )?;
        data.set_by_name("data", hex::encode(&self.inner.data.data))?;

        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )
    }
}
