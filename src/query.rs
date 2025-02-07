use pgrx::prelude::*;
use pgrx::{IntoDatum, PgOid};

use tokio::sync::OnceCell;

use crate::sync::svm::transactions::{
    SolanaInnerInstruction, SolanaInstruction,
};
use crate::types::*;

use alloy::core::hex;

use solana_client::rpc_client::SerializableTransaction;
use solana_sdk::bs58;

pub const EVM_BLOCK_COMPOSITE_TYPE: &str = "chainsync.EvmBlock";
pub const EVM_LOG_COMPOSITE_TYPE: &str = "chainsync.EvmLog";

pub const SVM_BLOCK_COMPOSITE_TYPE: &str = "chainsync.SvmBlock";
pub const SVM_INSTRUCTION_COMPOSITE_TYPE: &str = "chainsync.SvmInstruction";
pub const SVM_TRANSACTION_COMPOSITE_TYPE: &str = "chainsync.SvmTransaction";
pub const SVM_LOG_COMPOSITE_TYPE: &str = "chainsync.SvmLog";

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
                    svm_rpc: OnceCell::const_new(),
                    svm_ws: OnceCell::const_new(),
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

pub enum PgResult {
    None,
    Boolean(bool),
}

pub trait PgHandler {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error>;
}

impl PgHandler for u64 {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
        let found = Spi::get_one_with_args::<i64>(
        format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
        vec![
            (
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                (*self as i64).into_datum(),
            ),
            (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
        ],
      )?;

        Ok(PgResult::Boolean(found.is_some()))
    }
}

impl PgHandler for EvmBlock {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
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

        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )?;

        Ok(PgResult::None)
    }
}

impl PgHandler for EvmLog {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(EVM_LOG_COMPOSITE_TYPE).unwrap();

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
        )?;

        Ok(PgResult::None)
    }
}

impl PgHandler for SolanaBlock {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(SVM_BLOCK_COMPOSITE_TYPE).unwrap();

        data.set_by_name(
            "parent_slot",
            pgrx::AnyNumeric::try_from(self.parent_slot),
        )?;
        data.set_by_name(
            "block_height",
            pgrx::AnyNumeric::try_from(
                self.block_height.expect("Block height not in block"),
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
            pgrx::AnyNumeric::try_from(
                self.block_time.expect("Block time not in block"),
            ),
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
        )?;

        Ok(PgResult::None)
    }
}

impl PgHandler for SolanaLog {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(SVM_LOG_COMPOSITE_TYPE).unwrap();

        data.set_by_name(
            "slot_number",
            pgrx::AnyNumeric::try_from(self.context.slot),
        )?;
        data.set_by_name("signature", &self.value.signature)?;
        data.set_by_name("logs", self.value.logs.clone())?;

        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )?;

        Ok(PgResult::None)
    }
}

impl PgHandler for SolanaTransaction {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(SVM_TRANSACTION_COMPOSITE_TYPE)
                .unwrap();

        let decoded = self.transaction.transaction.decode().unwrap();

        data.set_by_name("signature", decoded.get_signature().to_string())?;
        data.set_by_name("slot", pgrx::AnyNumeric::try_from(self.slot))?;
        data.set_by_name(
            "block_time",
            pgrx::AnyNumeric::try_from(self.block_time.unwrap()),
        )?;

        let accounts: Vec<String> = decoded
            .message
            .static_account_keys()
            .iter()
            .map(|&key| key.to_string())
            .collect();
        data.set_by_name("accounts", accounts)?;

        let signatures: Vec<String> = decoded
            .signatures
            .into_iter()
            .map(|sig| sig.to_string())
            .collect();
        data.set_by_name("signatures", signatures)?;

        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )?;

        Ok(PgResult::None)
    }
}

impl PgHandler for SolanaInstruction<'_> {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(SVM_INSTRUCTION_COMPOSITE_TYPE)
                .unwrap();

        let decoded = self._tx.transaction.transaction.decode().unwrap();
        let meta = self._tx.transaction.meta.as_ref().unwrap();
        let loaded_addresses = meta.loaded_addresses.as_ref().unwrap();

        let mut accounts = decoded
            .message
            .static_account_keys()
            .iter()
            .map(|&key| key.to_string())
            .collect::<Vec<String>>();

        accounts.extend(loaded_addresses.writable.clone());
        accounts.extend(loaded_addresses.readonly.clone());

        data.set_by_name("signature", decoded.get_signature().to_string())?;
        data.set_by_name("slot", pgrx::AnyNumeric::try_from(self._tx.slot))?;
        data.set_by_name(
            "block_time",
            pgrx::AnyNumeric::try_from(self._tx.block_time.unwrap()),
        )?;

        data.set_by_name(
            "data",
            bs58::encode(&self._instruction.data).into_string(),
        )?;

        data.set_by_name(
            "program_id",
            &accounts[self._instruction.program_id_index as usize],
        )?;
        data.set_by_name(
            "accounts",
            self._instruction
                .accounts
                .iter()
                .map(|&acc| &accounts[acc as usize])
                .collect::<Vec<&String>>(),
        )?;

        data.set_by_name("index", self.index)?;

        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )?;

        Ok(PgResult::None)
    }
}

impl PgHandler for SolanaInnerInstruction<'_> {
    fn call_handler(
        &self,
        handler: &String,
        job: i64,
    ) -> Result<PgResult, pgrx::spi::Error> {
        let mut data =
            PgHeapTuple::new_composite_type(SVM_INSTRUCTION_COMPOSITE_TYPE)
                .unwrap();

        let decoded = self._tx.transaction.transaction.decode().unwrap();
        let meta = self._tx.transaction.meta.as_ref().unwrap();
        let loaded_addresses = meta.loaded_addresses.as_ref().unwrap();

        let mut accounts = decoded
            .message
            .static_account_keys()
            .iter()
            .map(|&key| key.to_string())
            .collect::<Vec<String>>();

        accounts.extend(loaded_addresses.writable.clone());
        accounts.extend(loaded_addresses.readonly.clone());

        data.set_by_name("signature", decoded.get_signature().to_string())?;
        data.set_by_name("slot", pgrx::AnyNumeric::try_from(self._tx.slot))?;
        data.set_by_name(
            "block_time",
            pgrx::AnyNumeric::try_from(self._tx.block_time.unwrap()),
        )?;

        data.set_by_name(
            "data",
            bs58::encode(&self._instruction.data).into_string(),
        )?;

        data.set_by_name(
            "program_id",
            &accounts[self._instruction.program_id_index as usize],
        )?;

        data.set_by_name(
            "accounts",
            self._instruction
                .accounts
                .iter()
                .map(|&acc| &accounts[acc as usize])
                .collect::<Vec<&String>>(),
        )?;

        data.set_by_name("index", self.index)?;
        data.set_by_name("inner_index", self.inner_index)?;

        Spi::run_with_args(
            format!("SELECT {}($1, (SELECT options FROM chainsync.jobs WHERE id = $2)::JSONB)", handler).as_str(),
            Some(vec![
                (
                    PgOid::Custom(data.composite_type_oid().unwrap()),
                    data.into_datum(),
                ),
                (PgOid::BuiltIn(PgBuiltInOids::INT4OID), job.into_datum()),
            ]),
        )?;

        Ok(PgResult::None)
    }
}
