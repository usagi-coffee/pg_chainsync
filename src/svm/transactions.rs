use pgrx::prelude::*;
use pgrx::{log, warning};

use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

use solana_client::rpc_client::SerializableTransaction;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_response::{Response, RpcLogsResponse};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::{
    UiCompiledInstruction, UiInnerInstructions, UiInstruction,
    UiTransactionEncoding,
};

use crate::svm::*;
use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

pub async fn handle_log(
    job: &Arc<Job>,
    log: &Response<RpcLogsResponse>,
    channel: &Channel,
) {
    let number = log.context.slot;
    log!(
        "sync: svm: transactions: {}: found {}",
        &job.name,
        &log.value.signature
    );

    // Await for block logic
    if matches!(job.options.await_block, Some(true)) {
        let (tx, rx) = oneshot::channel::<bool>();

        if channel.send(Message::CheckBlock(number, tx, Arc::clone(job))) {
            let found = rx.await;

            if found.unwrap() == false {
                // Retry finding block until available
                let mut retries = 0;
                loop {
                    if retries > 20 {
                        panic!(
                            "sync: svm: transactions: too many retries to get the block..."
                        );
                    }

                    if let Ok(block) = job
                        .connect_svm_rpc()
                        .await
                        .expect("Failed to retrieve rpc client")
                        .get_block_with_config(
                            number,
                            crate::svm::blocks::build_config(&job.options),
                        )
                        .await
                    {
                        channel.send(Message::SvmBlock(block, Arc::clone(job)));
                        break;
                    }

                    log!(
                        "sync: svm: transactions: could not find block {}, retrying",
                        number
                    );

                    sleep(Duration::from_millis(1000)).await;
                    retries = retries + 1;
                }
            }
        }
    }

    let signature = Signature::from_str(log.value.signature.as_str())
        .expect("Invalid signature");

    let tx = job
        .connect_svm_rpc()
        .await
        .expect("Failed retrieve rpc client")
        .get_transaction_with_config(&signature, build_config(&job.options))
        .await;

    match tx {
        Ok(tx) => {
            let meta =
                tx.transaction.meta.as_ref().expect("Failed to get meta");

            if meta.err.is_none() {
                channel.send(Message::SvmTransaction(tx, Arc::clone(job)));
            }
        }
        Err(err) => {
            warning!(
                "sync: svm: logs: {}: failed to get transaction {}",
                &job.name,
                err
            );
        }
    }
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

use crate::anyhow_pg_try;

pub fn handle_message(message: Message) {
    let Message::SvmTransaction(tx, job) = message else {
        return;
    };

    handle_transaction_message(tx, job)
}

pub fn handle_transaction_message(tx: SvmTransaction, job: Arc<Job>) {
    let id = job.id;
    let meta = tx.transaction.meta.as_ref().expect("Failed to get meta");
    let loaded_addresses =
        meta.loaded_addresses.as_ref().expect("No loaded addresses");
    let decoded = tx
        .transaction
        .transaction
        .decode()
        .expect("Failed to decode transaction");
    let mut accounts = decoded
        .message
        .static_account_keys()
        .iter()
        .map(|&key| key.to_string())
        .collect::<Vec<String>>();

    accounts.extend(loaded_addresses.writable.clone());
    accounts.extend(loaded_addresses.readonly.clone());

    let signature = decoded.get_signature();

    let inner_instructions: &Vec<UiInnerInstructions> = meta
        .inner_instructions
        .as_ref()
        .expect("No inner instructions");

    if let Some(instruction_handler) = &job.options.instruction_handler {
        for (i, instruction) in
            decoded.message.instructions().iter().enumerate()
        {
            // Inner instructions
            for inner in inner_instructions {
                if inner.index as usize != i {
                    break;
                }

                for (j, inner_instruction) in
                    inner.instructions.iter().enumerate()
                {
                    if let UiInstruction::Compiled(inner_instruction) =
                        inner_instruction
                    {
                        // Filter out program id if specified
                        if let Some(program_id) = job.options.program {
                            let inner_program = Pubkey::from_str(
                                &accounts[inner_instruction.program_id_index
                                    as usize],
                            );

                            if inner_program.unwrap() != program_id {
                                continue;
                            }
                        }

                        let bundled = SolanaInnerInstruction {
                            _tx: &tx,
                            _instruction: inner_instruction,
                            index: i as i16 + 1,
                            inner_index: j as i16 + 1,
                        };

                        log!(
                            "sync: svm: transactions: {}: adding {}<{}.{}>",
                            &job.name,
                            &signature,
                            bundled.index,
                            bundled.inner_index
                        );

                        if let Err(error) =
                            anyhow_pg_try!(|| bundled
                                .call_handler(&instruction_handler, id))
                        {
                            warning!("sync: svm: transactions: {}: instruction handler failed with {}", &id, error);
                        }
                    }
                }
            }

            // Filter out program id if specified
            if let Some(program_id) = job.options.program {
                let inner_program = Pubkey::from_str(
                    &accounts[instruction.program_id_index as usize],
                );

                if inner_program.unwrap() != program_id {
                    continue;
                }
            }

            let instruction = SolanaInstruction {
                _tx: &tx,
                _instruction: instruction,
                index: i as i16 + 1,
            };

            if let Err(error) = anyhow_pg_try!(
                || instruction.call_handler(&instruction_handler, id)
            ) {
                warning!("sync: svm: transactions: {}: instruction handler failed with {}", &id, error);
            }
        }
    }

    if let Some(transaction_handler) = &job.options.transaction_handler {
        log!(
            "sync: svm: transactions: {}: adding {}",
            &job.name,
            &signature
        );

        let id = job.id;
        if let Err(error) =
            anyhow_pg_try!(|| tx.call_handler(transaction_handler, id))
        {
            warning!(
              "sync: evm: transactions: {}: transaction handler failed with {}",
              &id,
              error
            );
        }
    };
}

pub fn build_config(_: &JobOptions) -> RpcTransactionConfig {
    RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base58),
        commitment: Some(CommitmentConfig::finalized()),
        max_supported_transaction_version: Some(0),
        ..Default::default()
    }
}

pub struct SolanaInstruction<'a> {
    pub _tx: &'a SvmTransaction,
    pub _instruction: &'a CompiledInstruction,
    pub index: i16,
}

pub struct SolanaInnerInstruction<'a> {
    pub _tx: &'a SvmTransaction,
    pub _instruction: &'a UiCompiledInstruction,
    pub index: i16,
    pub inner_index: i16,
}
