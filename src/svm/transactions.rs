use pgrx::prelude::*;
use pgrx::{log, warning};

use std::str::FromStr;
use std::sync::Arc;

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
) -> anyhow::Result<()> {
    log!(
        "sync: svm: transactions: {}: found {}",
        &job.name,
        &log.value.signature
    );

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
                bail!(
                    "sync: svm: transactions: {}: {}: transaction validation failed with {}",
                    &job.name,
                    &log.value.signature,
                    error
                );
            }
        },
        Err(err) => {
            bail!(
                "sync: svm: logs: {}: failed to get transaction {}",
                &job.name,
                err
            );
        }
    }

    Ok(())
}

use crate::query::PgHandler;
use pgrx::bgworkers::BackgroundWorker;

use crate::anyhow_pg_try;

pub fn handle_transaction_message(tx: SvmTransaction, job: Arc<Job>) {
    let Some(options) = &job.options.svm else {
        warning!(
            "sync: svm: transactions: {}: job options are not set",
            &job.name
        );
        return;
    };

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

    // Owners from the database
    let lookedup_owners = match &options.account_owner_lookup {
        Some(handler) => tx
            .accounts
            .iter()
            .map(|account| {
                anyhow_pg_try!(|| {
                    Job::return_handler_with_arg(account, handler, id as i32)
                })
                .ok()
            })
            .collect::<Vec<_>>(),
        None => tx.accounts.iter().map(|_| None).collect::<Vec<_>>(),
    };

    // Mints from the database
    let lookedup_mints = match &options.account_mint_lookup {
        Some(handler) => tx
            .accounts
            .iter()
            .map(|account| {
                anyhow_pg_try!(|| {
                    Job::return_handler_with_arg(account, handler, id as i32)
                })
                .ok()
            })
            .collect::<Vec<_>>(),
        None => tx.accounts.iter().map(|_| None).collect::<Vec<_>>(),
    };

    if let Some(instruction_handler) = &options.instruction_handler {
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
                    if let Some(program_id) = options.program {
                        let inner_program = Pubkey::from_str(
                            &tx.accounts
                                [inner_instruction.program_id_index as usize],
                        );

                        if inner_program.unwrap() != program_id {
                            continue;
                        }
                    }

                    // Filter out instruction discriminator if specified
                    if let Some(discriminators) =
                        &options.instruction_discriminators
                    {
                        let slice = bs58::decode(&inner_instruction.data)
                            .into_vec()
                            .unwrap();

                        let mut found = false;
                        for discriminator in discriminators {
                            if &slice[0] == discriminator {
                                found = true;
                                break;
                            }
                        }

                        if !found {
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

                                // Lookup initialized owners
                                if let Some(initialized) = tx
                                    .initialized_accounts
                                    .iter()
                                    .find(|initialized| initialized.address == tx.accounts[index])
                                {
                                    return Some(&initialized.owner);
                                }

                                if let Some(owner) = &lookedup_owners[index] {
                                    return Some(&owner);
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

                            // Lookup initialized mints
                            if let Some(initialized) = tx
                                .initialized_accounts
                                .iter()
                                .find(|initialized| {
                                    initialized.address == tx.accounts[index]
                                })
                            {
                                return Some(&initialized.mint);
                            }

                            if let Some(owner) = &lookedup_mints[index] {
                                return Some(&owner);
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
            if let Some(program_id) = options.program {
                if let Ok(program) = Pubkey::from_str(
                    &tx.accounts[instruction.program_id_index as usize],
                ) {
                    if program != program_id {
                        continue;
                    }
                }
            }

            // Filter out instruction discriminator if specified
            if let Some(discriminators) = &options.instruction_discriminators {
                let mut found = false;
                for discriminator in discriminators {
                    if &instruction.data[0] == discriminator {
                        found = true;
                        break;
                    }
                }

                if !found {
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

                    // Lookup initialized owners
                    if let Some(initialized) = tx
                        .initialized_accounts
                        .iter()
                        .find(|initialized| initialized.address == tx.accounts[index])
                    {
                        return Some(&initialized.owner);

                    }

                    if let Some(owner) = &lookedup_owners[index] {
                        return Some(&owner);
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

                    // Lookup initialized mints
                    if let Some(initialized) =
                        tx.initialized_accounts.iter().find(|initialized| {
                            initialized.address == tx.accounts[index]
                        })
                    {
                        return Some(&initialized.mint);
                    }

                    if let Some(mint) = &lookedup_owners[index] {
                        return Some(&mint);
                    }

                    None
                })
                .collect::<Vec<_>>();

            let instruction = SolanaInstruction {
                tx: &tx,
                instruction,
                accounts_owners,
                accounts_mints,
                index: i as i16 + 1,
            };

            log!(
                "sync: svm: transactions: {}: adding {}<{}.0>",
                &job.name,
                &tx.signature,
                i + 1
            );

            if let Err(error) = anyhow_pg_try!(
                || instruction.call_handler(&instruction_handler, id)
            ) {
                warning!("sync: svm: transactions: {}: instruction handler failed with {}", &id, error);
            }
        }
    }

    if let Some(transaction_handler) = &options.transaction_handler {
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
