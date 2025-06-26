use pgrx::prelude::*;
use pgrx::{log, warning};

use tokio::sync::Semaphore;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::ensure;

use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_response::{Response, RpcLogsResponse};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::{
    UiCompiledInstruction, UiInstruction, UiTransactionEncoding,
};

use crate::svm::*;
use crate::types::Job;

use crate::channel::Channel;
use crate::channel::unbounded;
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

    let signature = Signature::from_str(log.value.signature.as_str())?;

    let tx = job
        .connect_svm_rpc()
        .await?
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

pub async fn try_signatures(
    address: Pubkey,
    before: Option<Signature>,
    until: Option<Signature>,
    job: &Arc<Job>,
) -> anyhow::Result<Vec<Signature>, anyhow::Error> {
    let Some(_) = &job.options.svm else {
        bail!("sync: svm: tasks: {}: job options are not set", &job.name);
    };

    let rpc = job.connect_svm_rpc().await?;

    let mut before: Option<Signature> = before;
    let until: Option<Signature> = until;

    let mut signatures = vec![];
    let mut retries = 0;
    let mut batch = 0;
    loop {
        ensure!(
            retries < 10,
            "failed to get signatures after 10 retries, aborting..."
        );

        log!(
            "sync: svm: tasks: {}: getting signatures for {} [{}]",
            &job.name,
            address,
            batch,
        );

        match rpc.get_signatures_for_address_with_config(
            &address,
            solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                before,
                until,
                limit: Some(1000),
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig {
                    commitment: CommitmentLevel::Finalized,
                }),
            }).await {
                Ok(confirmed) => {
                  if confirmed.is_empty() {
                    break;
                  }

                  log!(
                    "sync: svm: tasks: {}: {}: got {} signatures",
                    &job.name,
                    address,
                    confirmed.len()
                  );

                  before = Signature::from_str(confirmed[confirmed.len() - 1].signature.as_str())?.into();
                  for status in confirmed {
                      let signature = Signature::from_str(status.signature.as_str())?;
                      signatures.push(signature);
                  }

                  retries = 0;
                  batch += 1;
                },
                Err(error) => {
                  warning!("sync: svm: tasks: {}: failed to get signatures with {}", &job.name, error);

                  sleep_until(
                      Instant::now() + Duration::from_millis(1000),
                  )
                  .await;
                  retries += 1;
                  continue;
            }
        }
    }

    Ok(signatures)
}

pub async fn try_transaction(
    signature: Signature,
    job: &Arc<Job>,
) -> anyhow::Result<SvmTransaction, anyhow::Error> {
    let config = transactions::build_config(&job.options);
    let rpc = job.connect_svm_rpc().await?;

    let mut retries = 0;
    loop {
        if retries > 20 {
            bail!("too many retries to get the block...");
        }

        match rpc.get_transaction_with_config(&signature, config).await {
            Ok(transaction) => {
                return transaction.try_into()
                    as Result<SvmTransaction, anyhow::Error>;
            }
            Err(error) => {
                warning!(
                    "sync: svm: {}: failed to get transaction {} with {}, retrying...",
                    &job.name,
                    &signature,
                    error
                );

                sleep_until(Instant::now() + Duration::from_millis(1000)).await;
                retries += 1;
            }
        };
    }
}

pub async fn stream_transactions(
    receiver: &mut Receiver<Signature>,
    sender: unbounded::OrderedSender<SvmTransaction>,
    job: Arc<Job>,
) -> Result<(), anyhow::Error> {
    let mut set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(SVM_RPC_PERMITS.get() as usize));
    let mut sequence = 0;

    while let Some(signature) = receiver.recv().await {
        if sender.closed() {
            break;
        }

        let id = sequence;
        sequence += 1;

        let permit = semaphore.clone().acquire_owned().await?;

        let job = job.clone();
        let sender = sender.clone();
        set.spawn(async move {
            match try_transaction(signature, &job).await {
                Ok(tx) => {
                    drop(permit);
                    sender.send(id, tx)?;

                    return Ok(());
                }
                Err(error) => {
                    sender.close(id);
                    return Err(error);
                }
            }
        });
    }

    // Only wait for the tasks to finish if the sender is still open
    if !sender.closed() {
        while let Some(result) = set.join_next().await {
            result??;
        }
    }

    Ok(())
}

use crate::query::PgHandler;
use crate::worker::SVM_RPC_PERMITS;
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

                            if let Some(balance) = balances[i]
                                && let OptionSerializer::Some(owner) =
                                    &balance.owner
                            {
                                return Some(owner);
                            }

                            // Lookup initialized owners
                            if let Some(initialized) = tx
                                .initialized_accounts
                                .iter()
                                .find(|initialized| {
                                    initialized.address == tx.accounts[index]
                                })
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

                            if let Some(mint) = &lookedup_mints[index] {
                                return Some(&mint);
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
                        warning!(
                            "sync: svm: transactions: {}: instruction handler failed with {}",
                            &id,
                            error
                        );
                    }
                }
            }

            // Filter out program id if specified
            if let Some(program_id) = options.program
                && let Ok(program) = Pubkey::from_str(
                    &tx.accounts[instruction.program_id_index as usize],
                )
                && program != program_id
            {
                continue;
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
                    if let Some(balance) = get_balance(*index)
                        && balance.owner.is_some()
                    {
                        return Some(balance);
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
                    if let Some(balance) = balances[i]
                        && let OptionSerializer::Some(owner) = &balance.owner
                    {
                        return Some(owner);
                    }

                    // Lookup initialized owners
                    if let Some(initialized) =
                        tx.initialized_accounts.iter().find(|initialized| {
                            initialized.address == tx.accounts[index]
                        })
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

                    if let Some(mint) = &lookedup_mints[index] {
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
                warning!(
                    "sync: svm: transactions: {}: instruction handler failed with {}",
                    &id,
                    error
                );
            }
        }
    }

    if let Some(transaction_handler) = &options.transaction_handler {
        log!(
            "sync: svm: transactions: {}: adding {}",
            &job.name,
            &tx.signature
        );

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
