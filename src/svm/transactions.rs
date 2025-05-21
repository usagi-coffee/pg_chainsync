use pgrx::prelude::*;
use pgrx::{log, warning};

use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_response::{Response, RpcLogsResponse};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::{
    UiCompiledInstruction, UiInstruction, UiTransactionEncoding,
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
        Ok(tx) => match tx.try_into() {
            Ok(tx) => {
                channel.send(Message::SvmTransaction(tx, Arc::clone(job)));
            }
            Err(error) => {
                warning!(
                    "sync: svm: transactions: {}: {}: transaction validation failed with {}",
                    &job.name,
                    &log.value.signature,
                    error
                );
            }
        },
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

pub fn handle_transaction_message(tx: SvmTransaction, job: Arc<Job>) {
    let id = job.id;

    let get_balance = |index: u8| {
        let mut found: Option<&UiTransactionTokenBalance> = None;

        // 1. Try post
        if let Some(balance) = tx
            .post_token_balances
            .iter()
            .find(|balance| balance.account_index == index)
        {
            if balance.owner.is_some() {
                return Some(balance);
            }

            found = Some(balance);
        }

        // 2. Try pre
        if let Some(balance) = tx
            .pre_token_balances
            .iter()
            .find(|balance| balance.account_index == index)
        {
            if balance.owner.is_some() {
                return Some(balance);
            }

            found = Some(balance);
        }

        found
    };

    if let Some(instruction_handler) = &job.options.instruction_handler {
        for (i, instruction) in tx.message.instructions().iter().enumerate() {
            for (j, inner_instruction) in tx
                .inner_instructions
                .iter()
                .find(|inner| inner.index == i as u8)
                .into_iter()
                .flat_map(|inner| inner.instructions.iter().enumerate())
            {
                if let UiInstruction::Compiled(inner_instruction) =
                    inner_instruction
                {
                    // Filter out program id if specified
                    if let Some(program_id) = job.options.program {
                        let inner_program = Pubkey::from_str(
                            &tx.accounts
                                [inner_instruction.program_id_index as usize],
                        );

                        if inner_program.unwrap() != program_id {
                            continue;
                        }
                    }

                    // Filter out instruction discriminator if specified
                    if let Some(discriminator) =
                        job.options.instruction_discriminator
                    {
                        let slice = bs58::decode(&inner_instruction.data)
                            .into_vec()
                            .unwrap();

                        if slice[0] != discriminator {
                            continue;
                        }
                    }

                    // Find a balance for each account if possible
                    let balances = inner_instruction
                        .accounts
                        .iter()
                        .map(|index| {
                            if let Some(balance) = get_balance(*index) {
                                if balance.owner.is_some() {
                                    return Some(balance);
                                }
                            }

                            None
                        })
                        .collect::<Vec<_>>();

                    let accounts_owners = inner_instruction
                            .accounts
                            .iter()
                            .enumerate()
                            .map(|(i, index)| {
                                let index = *index as usize;

                                if let Some(balance) = balances[i] {
                                    if let OptionSerializer::Some(owner) =
                                        &balance.owner
                                    {
                                        return Some(owner);
                                    }

                                    warning!("sync: svm: transactins: balance does not have an owner!");
                                }

                                // Lookup transient owners
                                if let Some(transient) = tx
                                    .transient_accounts
                                    .iter()
                                    .find(|transient| transient.account == tx.accounts[index])
                                {
                                    return Some(&transient.owner);
                                }

                                None
                            })
                            .collect::<Vec<_>>();

                    let accounts_mints = inner_instruction
                        .accounts
                        .iter()
                        .enumerate()
                        .map(|(i, index)| {
                            let index = *index as usize;

                            if let Some(balance) = balances[i] {
                                return Some(&balance.mint);
                            }

                            // Lookup transient mints
                            if let Some(transient) =
                                tx.transient_accounts.iter().find(|transient| {
                                    transient.account == tx.accounts[index]
                                })
                            {
                                return Some(&transient.mint);
                            }

                            None
                        })
                        .collect::<Vec<_>>();

                    let bundled = SolanaInnerInstruction {
                        tx: &tx,
                        instruction: inner_instruction,
                        accounts_owners,
                        accounts_mints,
                        index: i as i16 + 1,
                        inner_index: j as i16 + 1,
                    };

                    log!(
                        "sync: svm: transactions: {}: adding {}<{}.{}>",
                        &job.name,
                        &tx.signature,
                        bundled.index,
                        bundled.inner_index
                    );

                    if let Err(error) = anyhow_pg_try!(
                        || bundled.call_handler(&instruction_handler, id)
                    ) {
                        warning!("sync: svm: transactions: {}: instruction handler failed with {}", &id, error);
                    }
                }
            }

            // Filter out program id if specified
            if let Some(program_id) = job.options.program {
                if let Ok(program) = Pubkey::from_str(
                    &tx.accounts[instruction.program_id_index as usize],
                ) {
                    if program != program_id {
                        continue;
                    }
                }
            }

            // Filter out instruction discriminator if specified
            if let Some(discriminator) = job.options.instruction_discriminator {
                if instruction.data[0] != discriminator {
                    continue;
                }
            }

            // Find a balance for each account if possible
            let balances = instruction
                .accounts
                .iter()
                .map(|index| {
                    if let Some(balance) = get_balance(*index) {
                        if balance.owner.is_some() {
                            return Some(balance);
                        }
                    }

                    None
                })
                .collect::<Vec<_>>();

            let accounts_owners = instruction
                .accounts
                .iter()
                .enumerate()
                .map(|(i, index)| {
                    let index = *index as usize;
                    if let Some(balance) = balances[i] {
                        if let OptionSerializer::Some(owner) = &balance.owner {
                            return Some(owner);
                        }

                        warning!("sync: svm: transactins: balance does not have an owner!");
                    }

                    // Lookup transient owners
                    if let Some(transient) = tx
                        .transient_accounts
                        .iter()
                        .find(|transient| transient.account == tx.accounts[index])
                    {
                        return Some(&transient.owner);
                    }

                    None
                })
                .collect::<Vec<_>>();

            let accounts_mints = instruction
                .accounts
                .iter()
                .enumerate()
                .map(|(i, index)| {
                    let index = *index as usize;
                    if let Some(balance) = balances[i] {
                        return Some(&balance.mint);
                    }

                    // Lookup transient mints
                    if let Some(transient) =
                        tx.transient_accounts.iter().find(|transient| {
                            transient.account == tx.accounts[index]
                        })
                    {
                        return Some(&transient.mint);
                    }

                    None
                })
                .collect::<Vec<_>>();

            let instruction = SolanaInstruction {
                tx: &tx,
                instruction: instruction,
                accounts_owners,
                accounts_mints,
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
            &tx.signature
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
    pub tx: &'a SvmTransaction,
    pub instruction: &'a CompiledInstruction,
    pub accounts_owners: Vec<Option<&'a String>>,
    pub accounts_mints: Vec<Option<&'a String>>,
    pub index: i16,
}

pub struct SolanaInnerInstruction<'a> {
    pub tx: &'a SvmTransaction,
    pub instruction: &'a UiCompiledInstruction,
    pub accounts_owners: Vec<Option<&'a String>>,
    pub accounts_mints: Vec<Option<&'a String>>,
    pub index: i16,
    pub inner_index: i16,
}
