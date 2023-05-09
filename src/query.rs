use ethers::abi::AbiEncode;
use pgx::prelude::*;
use pgx::{IntoDatum, PgOid};

use ethers::prelude::*;

use crate::types::*;

pub const BLOCK_COMPOSITE_TYPE: &str = "chainsync.Block";

impl Job {
    pub fn register(
        job_type: JobType,
        chain_id: i64,
        provider_url: &str,
        callback: &str,
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
            ]),
        )
        .is_ok()
    }

    pub fn query_all() -> Result<Vec<Job>, pgx::spi::Error> {
        Spi::connect(|client| {
            let mut table =
                client.select("SELECT * FROM chainsync.jobs", None, None)?;

            let mut jobs: Vec<Job> = Vec::new();
            while table.next().is_some() {
                let job_type =
                    table.get_by_name::<String, &'static str>("job_type");

                if job_type.is_err() {
                    continue;
                }

                jobs.push(Job {
                    job_type: job_type.unwrap().unwrap().parse().unwrap(),
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
                    ws: None,
                });
            }

            Ok(jobs)
        })
    }
}

pub fn call_block_handler(
    callback: &String,
    block: &Block<H256>,
) -> Result<(), pgx::spi::Error> {
    let mut data =
        PgHeapTuple::new_composite_type(BLOCK_COMPOSITE_TYPE).unwrap();

    data.set_by_name("hash", block.hash.unwrap_or_default().encode_hex())?;
    data.set_by_name("number", block.number.unwrap().as_u64() as i64)?;
    data.set_by_name(
        "timestamp",
        pgx::TimestampWithTimeZone::try_from(block.timestamp.as_u64() as i64),
    )?;
    data.set_by_name("author", block.author.unwrap_or_default().encode_hex())?;
    data.set_by_name("state_root", block.state_root.encode_hex())?;
    data.set_by_name("parent_hash", block.parent_hash.encode_hex())?;
    data.set_by_name("uncles_hash", block.uncles_hash.encode_hex())?;
    data.set_by_name(
        "transactions_root",
        block.transactions_root.encode_hex(),
    )?;

    data.set_by_name("receipts_root", block.receipts_root.encode_hex())?;
    data.set_by_name("gas_used", block.gas_used.as_u64() as i64)?;
    data.set_by_name("gas_limit", block.gas_limit.as_u64() as i64)?;
    data.set_by_name(
        "timestamp",
        pgx::TimestampWithTimeZone::try_from(block.timestamp.as_u64() as i64),
    )?;
    data.set_by_name("difficulty", block.difficulty.as_u64() as i64)?;
    data.set_by_name(
        "total_difficulty",
        block.total_difficulty.unwrap_or_default().as_u64() as i64,
    )?;
    data.set_by_name("size", block.size.unwrap().as_u64() as i64)?;

    Spi::run_with_args(
        format!("SELECT {}($1)", callback).as_str(),
        Some(vec![(
            PgOid::Custom(data.composite_type_oid().unwrap()),
            data.into_datum(),
        )]),
    )
}
