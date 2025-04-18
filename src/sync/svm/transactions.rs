use pgrx::prelude::*;
use pgrx::{log, warning};

use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::{sleep, sleep_until, timeout, Duration, Instant};
use tokio_stream::{StreamExt, StreamMap, StreamNotifyClose};

use bus::BusReader;

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

use crate::types::svm::*;
use crate::types::Job;

use crate::channel::Channel;
use crate::types::*;

pub async fn listen(channel: Arc<Channel>, mut signals: BusReader<Signal>) {
    'sync: loop {
        let (tx, rx) = oneshot::channel::<Vec<Job>>();
        if !channel.send(Message::Jobs(tx)) {
            break;
        }

        if let Ok(jobs) = rx.await {
            let mut jobs = jobs
                .svm_jobs()
                .transaction_jobs()
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>();

            let mut map = StreamMap::new();

            for job in jobs.iter_mut() {
                match job.connect_svm_ws().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!(
                            "sync: svm: transactions: {}: ws: {}",
                            job.id,
                            err
                        );
                        continue 'sync;
                    }
                }

                match job.connect_svm_rpc().await {
                    Ok(_) => {}
                    Err(err) => {
                        warning!(
                            "sync: svm: transactions: {}: rpc: {}",
                            job.id,
                            err
                        );
                        continue 'sync;
                    }
                }
            }

            for i in 0..jobs.len() {
                let job = &jobs[i];

                match crate::sync::svm::logs::build_stream(&job).await {
                    Ok(stream) => {
                        log!(
                            "sync: svm: transactions: {} started listening",
                            job.id
                        );
                        channel.send(Message::UpdateJob(
                            JobStatus::Running,
                            Arc::clone(job),
                        ));

                        map.insert(i, StreamNotifyClose::new(stream));
                    }
                    Err(err) => {
                        warning!(
                            "sync: svm: transactions: {}: {}",
                            job.id,
                            err
                        );
                        continue 'sync;
                    }
                }
            }

            log!(
                "sync: svm: transactions: started listening to {} jobs",
                jobs.len()
            );

            let mut drain = false;
            loop {
                match signals.try_recv() {
                    Ok(signal) => {
                        if signal == Signal::RestartTransactions {
                            drain = true;
                        }
                    }
                    Err(..) => {}
                }

                if map.is_empty() {
                    sleep_until(Instant::now() + Duration::from_millis(100))
                        .await;

                    if drain {
                        continue 'sync;
                    }
                    continue;
                }

                match timeout(Duration::from_secs(1), map.next()).await {
                    Ok(Some(tick)) => {
                        let (i, log) = tick;
                        let job = &jobs[i];

                        if log.is_none() {
                            warning!(
                                "sync: svm: transactions: stream {} has ended, restarting providers",
                                job.id
                            );
                            continue 'sync;
                        }

                        // SAFETY: unwrap is safe because we checked for None
                        handle_log(job, log.unwrap(), &channel).await;
                    }
                    // If it took more than 1 second the stream is probably drained so we can "almost"
                    // safely restart the providers
                    _ => {
                        if drain {
                            continue 'sync;
                        }
                    }
                }
            }
        }
    }
}

pub async fn handle_log(
    job: &Arc<Job>,
    log: Response<RpcLogsResponse>,
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
                            crate::sync::svm::blocks::build_config(
                                &job.options,
                            ),
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
    log!("sync: svm: transactions: {}: adding {}", "sol", &signature);

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
                    match inner_instruction {
                        UiInstruction::Compiled(inner_instruction) => {
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
                                index: i as i16,
                                inner_index: j as i16,
                            };

                            BackgroundWorker::transaction(|| {
                                PgTryBuilder::new(|| {
                                  bundled.call_handler(&instruction_handler, id).expect(
                                      "sync: svm: transactions: failed to call the handler {}",
                                  );
                              })
                              .catch_rust_panic(|e| {
                                  log!("{:?}", e);
                                  warning!(
                                      "sync: svm: transactions: failed to call handler for {}",
                                      &signature
                                  );
                              })
                              .catch_others(|e| {
                                  log!("{:?}", e);
                                  warning!(
                                      "sync: svm: transactions: handler failed to put {}",
                                      &signature
                                  );
                              })
                              .execute();
                            });
                        }
                        _ => {
                            log!(
                              "sync: svm: transactions: {}: adding inner instruction {:?}",
                              &id,
                              inner_instruction
                            );
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
                index: i as i16,
            };

            // Outer instruction
            BackgroundWorker::transaction(|| {
                PgTryBuilder::new(|| {
                    instruction.call_handler(&instruction_handler, id).expect(
                        "sync: svm: transactions: failed to call the handler {}",
                    );
                })
                .catch_rust_panic(|e| {
                    log!("{:?}", e);
                    warning!(
                        "sync: svm: transactions: failed to call handler for {}",
                        &signature
                    );
                })
                .catch_others(|e| {
                    log!("{:?}", e);
                    warning!(
                        "sync: svm: transactions: handler failed to put {}",
                        &signature
                    );
                })
                .execute();
            });
        }
    }

    let transaction_handler = job
        .options
        .transaction_handler
        .as_ref()
        .expect("sync: svm: transactions: missing handler");

    let id = job.id;

    // Call instruction handler
    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            tx.call_handler(&transaction_handler, id).expect(
                "sync: svm: transactions: failed to call the handler {}",
            );
        })
        .catch_rust_panic(|e| {
            log!("{:?}", e);
            warning!(
                "sync: svm: transactions: failed to call handler for {}",
                &signature
            );
        })
        .catch_others(|e| {
            log!("{:?}", e);
            warning!(
                "sync: svm: transactions: handler failed to put {}",
                &signature
            );
        })
        .execute();
    });
}

pub fn build_config(_: &JobOptions) -> RpcTransactionConfig {
    RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Binary),
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
