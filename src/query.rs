use pgrx::prelude::*;
use pgrx::{IntoDatum, PgOid};

use alloy::core::hex;
use alloy::rpc::types::Log;
use alloy_chains::Chain;

use crate::types::*;

pub const BLOCK_COMPOSITE_TYPE: &str = "chainsync.Block";
pub const LOG_COMPOSITE_TYPE: &str = "chainsync.Log";

pub trait PgHandler {
    fn call_handler(
        &self,
        chain: &Chain,
        callback: &String,
        job_id: &i64,
    ) -> Result<(), pgrx::spi::Error>;
}

impl Job {
    pub fn register(
        job_type: JobKind,
        chain_id: i64,
        provider_url: &str,
        callback: &str,
        oneshot: bool,
        preload: bool,
        cron: Option<&str>,
        options: pgrx::JsonB,
    ) -> i64 {
        match Spi::get_one_with_args(
            include_str!("../sql/insert_job.sql"),
            vec![
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
                (PgOid::BuiltIn(PgBuiltInOids::BOOLOID), oneshot.into_datum()),
                (PgOid::BuiltIn(PgBuiltInOids::BOOLOID), preload.into_datum()),
                (PgOid::BuiltIn(PgBuiltInOids::TEXTOID), cron.into_datum()),
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
            include_str!("../sql/update_job.sql"),
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
                let kind = table.get_by_name::<String, &'static str>("kind");

                if kind.is_err() {
                    continue;
                }

                let options = table
                    .get_by_name::<pgrx::JsonB, &'static str>("options")
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
                    oneshot: table
                        .get_by_name::<bool, &'static str>("oneshot")
                        .unwrap()
                        .unwrap(),
                    preload: table
                        .get_by_name::<bool, &'static str>("preload")
                        .unwrap()
                        .unwrap(),
                    cron: table
                        .get_by_name::<String, &'static str>("cron")
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
        job_id: &i64,
    ) -> Result<(), pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(BLOCK_COMPOSITE_TYPE).unwrap();

        data.set_by_name("chain", chain.id() as i64)?;
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
            format!("SELECT {}($1, $2)", callback).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT8OID), job_id.into_datum()),
            ]),
        )
    }
}

impl PgHandler for Log {
    fn call_handler(
        &self,
        chain: &Chain,
        callback: &String,
        job_id: &i64,
    ) -> Result<(), pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(LOG_COMPOSITE_TYPE).unwrap();

        data.set_by_name("chain", chain.id() as i64)?;
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
            format!("SELECT {}($1, $2)", callback).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT8OID), job_id.into_datum()),
            ]),
        )
    }
}
