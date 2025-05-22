pub mod blocks;
pub mod logs;
pub mod tasks;
pub mod transactions;

use std::str::FromStr;
use std::sync::Arc;

use anyhow::bail;

use solana_client::rpc_client::SerializableTransaction;
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::option_serializer::OptionSerializer;
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use solana_transaction_status_client_types::TransactionDetails;
use solana_transaction_status_client_types::UiConfirmedBlock;
use solana_transaction_status_client_types::UiInnerInstructions;
use solana_transaction_status_client_types::UiInstruction;
use solana_transaction_status_client_types::UiTransactionTokenBalance;

use crate::types::Job;

pub type SvmPubSub = solana_client::nonblocking::pubsub_client::PubsubClient;
pub type SvmPubSubError = solana_client::pubsub_client::PubsubClientError;
pub type SvmRpc = solana_client::nonblocking::rpc_client::RpcClient;

pub type SvmBlock = UiConfirmedBlock;
pub type SvmLog = solana_client::rpc_response::Response<
    solana_client::rpc_response::RpcLogsResponse,
>;

pub type SvmTransactionDetails = TransactionDetails;
pub type RawSvmTransaction = EncodedConfirmedTransactionWithStatusMeta;

const SPL_TOKEN_PROGRAM: Pubkey =
    pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

const SPL_INITIALIZE_ACCOUNT: u8 = 1;
const SPL_INITIALIZE_ACCOUNT3: u8 = 18;

pub struct SvmTransaction {
    pub signature: Signature,
    pub slot: u64,
    pub block_time: i64,
    pub signatures: Vec<Signature>,
    pub message: VersionedMessage,
    pub accounts: Vec<String>,
    pub writable_accounts: Vec<String>,
    pub initialized_accounts: Vec<InitializedAccount>,
    pub inner_instructions: Vec<UiInnerInstructions>,
    pub pre_token_balances: Vec<UiTransactionTokenBalance>,
    pub post_token_balances: Vec<UiTransactionTokenBalance>,
    pub failed: bool,
}

impl Job {
    pub async fn connect_svm_ws(
        &self,
    ) -> anyhow::Result<&Arc<SvmPubSub>, SvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");

        self.svm_ws
            .get_or_try_init(|| async {
                let r = SvmPubSub::new(&url);
                r.await.map(Arc::new)
            })
            .await
    }

    pub async fn reconnect_svm_ws(
        &self,
    ) -> anyhow::Result<SvmPubSub, SvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");

        SvmPubSub::new(&url).await
    }

    pub async fn connect_svm_rpc(&self) -> anyhow::Result<&Arc<SvmRpc>> {
        let url = self
            .options
            .rpc
            .as_ref()
            .expect("RPC URL was not provided")
            .clone();

        self.svm_rpc
            .get_or_try_init(|| async { Ok(Arc::new(SvmRpc::new(url))) })
            .await
    }

    pub async fn reconnect_svm_rpc(&self) -> SvmRpc {
        let url = self
            .options
            .rpc
            .as_ref()
            .expect("RPC URL was not provided")
            .clone();

        SvmRpc::new(url)
    }
}

#[derive(Debug, Clone)]
pub struct InitializedAccount {
    pub account: String,
    pub owner: String,
    pub mint: String,
}

impl TryInto<SvmTransaction> for RawSvmTransaction {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<SvmTransaction, Self::Error> {
        let Some(block_time) = self.block_time else {
            bail!("block time was not in transaction");
        };

        let Some(transaction) = self.transaction.transaction.decode() else {
            bail!("transaction could not be decoded");
        };

        let Some(meta) = self.transaction.meta else {
            bail!("meta was not in transaction");
        };

        let OptionSerializer::Some(loaded_addresses) = meta.loaded_addresses
        else {
            bail!("loaded addresses was not in transaction");
        };

        let OptionSerializer::Some(inner_instructions) =
            meta.inner_instructions
        else {
            bail!("inner instructions were not in transaction");
        };

        let mut accounts = transaction
            .message
            .static_account_keys()
            .iter()
            .map(|&key| key.to_string())
            .collect::<Vec<String>>();

        accounts.extend(loaded_addresses.writable.clone());
        accounts.extend(loaded_addresses.readonly.clone());

        let mut initialized_accounts = vec![];

        for (i, instruction) in
            transaction.message.instructions().iter().enumerate()
        {
            for inner_instruction in inner_instructions
                .iter()
                .find(|inner| inner.index == i as u8)
                .into_iter()
                .flat_map(|inner| inner.instructions.iter())
            {
                if let UiInstruction::Compiled(inner_instruction) =
                    inner_instruction
                {
                    let inner_program = Pubkey::from_str(
                        &accounts[inner_instruction.program_id_index as usize],
                    )
                    .expect("account index out of bounds");

                    if inner_program != SPL_TOKEN_PROGRAM {
                        continue;
                    }

                    let slice = bs58::decode(&inner_instruction.data)
                        .into_vec()
                        .expect("failed to decode data");

                    if slice[0] == SPL_INITIALIZE_ACCOUNT {
                        initialized_accounts.push(InitializedAccount {
                            account: accounts
                                [inner_instruction.accounts[0] as usize]
                                .to_owned(),
                            mint: accounts
                                [inner_instruction.accounts[1] as usize]
                                .to_owned(),
                            owner: accounts
                                [inner_instruction.accounts[2] as usize]
                                .to_owned(),
                        });
                    } else if slice[0] == SPL_INITIALIZE_ACCOUNT3 {
                        initialized_accounts.push(InitializedAccount {
                            account: accounts
                                [inner_instruction.accounts[0] as usize]
                                .to_owned(),
                            mint: accounts
                                [inner_instruction.accounts[1] as usize]
                                .to_owned(),
                            owner: bs58::encode(&slice[1..33]).into_string(),
                        });
                    }
                }
            }

            // We care only about SPL Token program
            let program = Pubkey::from_str(
                &accounts[instruction.program_id_index as usize],
            )
            .expect("account index out of bounds");

            if program != SPL_TOKEN_PROGRAM {
                continue;
            }

            if let Some(discriminator) = instruction.data.get(0) {
                if discriminator == &SPL_INITIALIZE_ACCOUNT {
                    initialized_accounts.push(InitializedAccount {
                        account: accounts[instruction.accounts[0] as usize]
                            .to_owned(),
                        mint: accounts[instruction.accounts[1] as usize]
                            .to_owned(),
                        owner: accounts[instruction.accounts[2] as usize]
                            .to_owned(),
                    });
                } else if discriminator == &SPL_INITIALIZE_ACCOUNT3 {
                    initialized_accounts.push(InitializedAccount {
                        account: accounts[instruction.accounts[0] as usize]
                            .to_owned(),
                        mint: accounts[instruction.accounts[1] as usize]
                            .to_owned(),
                        owner: bs58::encode(&instruction.data[1..33])
                            .into_string(),
                    });
                }
            }
        }

        Ok(SvmTransaction {
            signature: *transaction.get_signature(),
            slot: self.slot,
            block_time,
            signatures: transaction.signatures,
            message: transaction.message,
            writable_accounts: loaded_addresses.writable,
            initialized_accounts,
            accounts,
            inner_instructions,
            pre_token_balances: meta.pre_token_balances.unwrap_or(vec![]),
            post_token_balances: meta.post_token_balances.unwrap_or(vec![]),
            failed: meta.err.is_some(),
        })
    }
}
