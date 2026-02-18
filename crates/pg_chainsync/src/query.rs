use pgrx::datum::{DatumWithOid, IntoDatum};
use pgrx::prelude::*;

use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::Arc;
use std::sync::{OnceLock, RwLock};

use anyhow::anyhow;

use tokio::sync::OnceCell;

use crate::evm::*;
use crate::svm::transactions::{SolanaInnerInstruction, SolanaInstruction};
use crate::svm::*;
use crate::types::{PostgresReturn, *};

use alloy::core::hex;

use solana_sdk::bs58;

pub const EVM_BLOCK_COMPOSITE_TYPE: &str = "chainsync.EvmBlock";
pub const EVM_LOG_COMPOSITE_TYPE: &str = "chainsync.EvmLog";

pub const SVM_BLOCK_COMPOSITE_TYPE: &str = "chainsync.SvmBlock";
pub const SVM_INSTRUCTION_COMPOSITE_TYPE: &str = "chainsync.SvmInstruction";
pub const SVM_TRANSACTION_COMPOSITE_TYPE: &str = "chainsync.SvmTransaction";
pub const SVM_LOG_COMPOSITE_TYPE: &str = "chainsync.SvmLog";
pub const SVM_ACCOUNT_COMPOSITE_TYPE: &str = "chainsync.SvmAccount";

static JOBS: OnceLock<RwLock<Vec<Job>>> = OnceLock::new();

fn jobs_store() -> &'static RwLock<Vec<Job>> {
    JOBS.get_or_init(|| RwLock::new(Vec::new()))
}

fn options_for_job(job: i32) -> Result<pgrx::JsonB, anyhow::Error> {
    let jobs = jobs_store().read().expect("jobs store read");
    let Some(found) = jobs.iter().find(|j| j.id == job as i64) else {
        return Err(anyhow!("job {} not found", job));
    };
    Ok(pgrx::JsonB(serde_json::to_value(&found.options)?))
}

impl Job {
    pub fn replace_all(mut jobs: Vec<Job>) {
        for job in &mut jobs {
            job.evm = OnceCell::const_new();
            job.svm_rpc = OnceCell::const_new();
            job.svm_ws = OnceCell::const_new();
        }
        let mut guard = jobs_store().write().expect("jobs store write");
        *guard = jobs;
    }

    pub fn update(id: i64, status: JobStatus) -> Result<(), anyhow::Error> {
        let status_text: String = status.into();
        let mut jobs = jobs_store().write().expect("jobs store write");
        let Some(job) = jobs.iter_mut().find(|job| job.id == id) else {
            return Err(anyhow!("job {} not found", id));
        };
        job.status = status_text;
        Ok(())
    }

    pub fn query_all() -> Result<Vec<Job>, anyhow::Error> {
        Ok(jobs_store().read().expect("jobs store read").clone())
    }

    pub fn handler(handler: &Arc<str>, job: i32) -> Result<(), anyhow::Error> {
        let options = options_for_job(job)?;
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![DatumWithOid::from(job), DatumWithOid::from(options)],
        )
        .map_err(|e| e.into())
    }

    pub fn return_handler<R: FromDatum + IntoDatum, S: std::fmt::Display>(
        handler: S,
        job: i32,
    ) -> Result<R, anyhow::Error> {
        let options = options_for_job(job)?;
        Spi::get_one_with_args::<R>(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![DatumWithOid::from(job), DatumWithOid::from(options)],
        )
        .map_err(|e| e.into())
        .and_then(|opt| opt.ok_or_else(|| anyhow!("did not return anything")))
    }

    pub fn return_handler_with_arg<
        T: IntoDatum + UnwindSafe + RefUnwindSafe,
        R: FromDatum + IntoDatum,
        S: std::fmt::Display,
    >(
        arg: T,
        handler: S,
        job: i32,
    ) -> Result<R, anyhow::Error> {
        let options = options_for_job(job)?;
        Spi::get_one_with_args::<R>(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![DatumWithOid::from(arg), DatumWithOid::from(options)],
        )
        .map_err(|e| e.into())
        .and_then(|opt| opt.ok_or_else(|| anyhow!("did not return anything")))
    }
}

pub trait PgHandler {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error>;
}

impl PgHandler for u64 {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        Spi::get_one_with_args::<i64>(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                DatumWithOid::from(*self as i64),
                DatumWithOid::from(options),
            ],
        )
        .map(|e| PostgresReturn::Boolean(e.is_some()))
        .map_err(|e| e.into())
    }
}

impl PgHandler for EvmBlock {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(EVM_BLOCK_COMPOSITE_TYPE).unwrap();

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

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}

impl PgHandler for EvmLog {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(EVM_LOG_COMPOSITE_TYPE).unwrap();

        data.set_by_name(
            "block_number",
            pgrx::AnyNumeric::try_from(
                self.block_number.unwrap().to_string().as_str(),
            ),
        )?;

        if let Some(block_timestamp) = self.block_timestamp {
            data.set_by_name(
                "block_timestamp",
                pgrx::AnyNumeric::try_from(block_timestamp),
            )?;
        }

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

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}

impl PgHandler for SvmBlock {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(SVM_BLOCK_COMPOSITE_TYPE).unwrap();

        data.set_by_name(
            "parent_slot",
            pgrx::AnyNumeric::try_from(self.parent_slot),
        )?;
        data.set_by_name(
            "block_height",
            pgrx::AnyNumeric::try_from(
                self.block_height.expect("block height in a block"),
            ),
        )?;
        data.set_by_name("block_hash", self.blockhash.clone())?;
        data.set_by_name(
            "previous_block_hash",
            self.previous_blockhash.clone(),
        )?;
        data.set_by_name(
            "signatures",
            self.signatures.clone().unwrap_or(vec![]),
        )?;

        data.set_by_name(
            "block_time",
            self.block_time.expect("block time in a block"),
        )?;

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}

impl PgHandler for SvmLog {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(SVM_LOG_COMPOSITE_TYPE).unwrap();

        data.set_by_name(
            "slot_number",
            pgrx::AnyNumeric::try_from(self.context.slot),
        )?;
        data.set_by_name("signature", &self.value.signature)?;
        data.set_by_name("logs", self.value.logs.clone())?;

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}

impl PgHandler for SvmTransaction {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(SVM_TRANSACTION_COMPOSITE_TYPE)
                .unwrap();

        data.set_by_name("signature", self.signature.to_string())?;
        data.set_by_name("slot", pgrx::AnyNumeric::try_from(self.slot))?;
        data.set_by_name("block_time", self.block_time)?;
        data.set_by_name("accounts", self.accounts.clone())?;

        let signatures: Vec<String> = self
            .signatures
            .clone()
            .into_iter()
            .map(|sig| sig.to_string())
            .collect::<Vec<String>>();

        data.set_by_name("signatures", signatures)?;

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}

impl PgHandler for SolanaInstruction<'_> {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(SVM_INSTRUCTION_COMPOSITE_TYPE)
                .unwrap();

        data.set_by_name("signature", self.tx.signature.to_string())?;
        data.set_by_name("slot", pgrx::AnyNumeric::try_from(self.tx.slot))?;
        data.set_by_name("block_time", self.tx.block_time)?;

        data.set_by_name("data", self.instruction.data.as_slice())?;

        data.set_by_name(
            "program_id",
            &self.tx.accounts[self.instruction.program_id_index as usize],
        )?;

        data.set_by_name(
            "accounts",
            self.instruction
                .accounts
                .iter()
                .map(|&acc| &self.tx.accounts[acc as usize])
                .collect::<Vec<&String>>(),
        )?;
        data.set_by_name("accounts_owners", self.accounts_owners.clone())?;
        data.set_by_name("accounts_mints", self.accounts_mints.clone())?;

        data.set_by_name("index", self.index)?;
        data.set_by_name("inner_index", 0)?;

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}

impl PgHandler for SolanaInnerInstruction<'_> {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(SVM_INSTRUCTION_COMPOSITE_TYPE)
                .unwrap();

        data.set_by_name("signature", self.tx.signature.to_string())?;
        data.set_by_name("slot", pgrx::AnyNumeric::try_from(self.tx.slot))?;
        data.set_by_name("block_time", self.tx.block_time)?;

        data.set_by_name(
            "data",
            bs58::decode(&self.instruction.data).into_vec()?.as_slice(),
        )?;

        data.set_by_name(
            "program_id",
            &self.tx.accounts[self.instruction.program_id_index as usize],
        )?;

        data.set_by_name(
            "accounts",
            self.instruction
                .accounts
                .iter()
                .map(|&acc| &self.tx.accounts[acc as usize])
                .collect::<Vec<&String>>(),
        )?;
        data.set_by_name("accounts_owners", self.accounts_owners.clone())?;
        data.set_by_name("accounts_mints", self.accounts_mints.clone())?;

        data.set_by_name("index", self.index)?;
        data.set_by_name("inner_index", self.inner_index)?;

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}

impl PgHandler for SvmAccount {
    fn call_handler(
        &self,
        handler: &str,
        job: i64,
    ) -> Result<PostgresReturn, anyhow::Error> {
        let options = options_for_job(job as i32)?;
        let mut data =
            PgHeapTuple::new_composite_type(SVM_ACCOUNT_COMPOSITE_TYPE)
                .unwrap();

        data.set_by_name("pubkey", self.address.to_string())?;
        data.set_by_name("data", self.inner.data.as_slice())?;
        data.set_by_name("executable", self.inner.executable)?;
        data.set_by_name("owner", self.inner.owner.to_string())?;
        data.set_by_name(
            "lamports",
            pgrx::AnyNumeric::try_from(self.inner.rent_epoch),
        )?;
        data.set_by_name(
            "rent_epoch",
            pgrx::AnyNumeric::try_from(self.inner.rent_epoch),
        )?;

        let oid = data.composite_type_oid().unwrap();
        Spi::run_with_args(
            format!("SELECT {}($1, $2)", handler).as_str(),
            &vec![
                unsafe { DatumWithOid::new(data, oid) },
                DatumWithOid::from(options),
            ],
        )
        .map(|_| PostgresReturn::Void)
        .map_err(|e| e.into())
    }
}
