pub mod blocks;
pub mod logs;
pub mod tasks;
pub mod transactions;

use std::sync::Arc;

use anyhow::bail;

use solana_client::rpc_client::SerializableTransaction;
use solana_sdk::message::VersionedMessage;
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::option_serializer::OptionSerializer;
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use solana_transaction_status_client_types::TransactionDetails;
use solana_transaction_status_client_types::UiConfirmedBlock;
use solana_transaction_status_client_types::UiInnerInstructions;
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

pub struct SvmTransaction {
    pub signature: Signature,
    pub slot: u64,
    pub block_time: i64,
    pub signatures: Vec<Signature>,
    pub message: VersionedMessage,
    pub writable_accounts: Vec<String>,
    pub accounts: Vec<String>,
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

        Ok(SvmTransaction {
            signature: *transaction.get_signature(),
            slot: self.slot,
            block_time,
            signatures: transaction.signatures,
            message: transaction.message,
            writable_accounts: loaded_addresses.writable,
            accounts,
            inner_instructions,
            pre_token_balances: meta.pre_token_balances.unwrap_or(vec![]),
            post_token_balances: meta.post_token_balances.unwrap_or(vec![]),
            failed: meta.err.is_some(),
        })
    }
}
