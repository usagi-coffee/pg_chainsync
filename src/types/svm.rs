use std::sync::Arc;

use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use solana_transaction_status_client_types::TransactionDetails;
use solana_transaction_status_client_types::UiConfirmedBlock;

pub type SolanaPubSub = solana_client::nonblocking::pubsub_client::PubsubClient;
pub type SolanaPubSubError = solana_client::pubsub_client::PubsubClientError;
pub type SolanaRpc = solana_client::nonblocking::rpc_client::RpcClient;

pub type SolanaBlock = UiConfirmedBlock;
pub type SolanaLog = solana_client::rpc_response::Response<
    solana_client::rpc_response::RpcLogsResponse,
>;
pub type SolanaTransaction = EncodedConfirmedTransactionWithStatusMeta;
pub type SolanaTransactionDetails = TransactionDetails;

use crate::types::Job;

impl Job {
    pub async fn connect_svm_ws(
        &self,
    ) -> anyhow::Result<&Arc<SolanaPubSub>, SolanaPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");

        self.svm_ws
            .get_or_try_init(|| async {
                let r = SolanaPubSub::new(&url);
                r.await.map(Arc::new)
            })
            .await
    }

    pub async fn connect_svm_rpc(&self) -> anyhow::Result<&Arc<SolanaRpc>> {
        let url = self
            .options
            .rpc
            .as_ref()
            .expect("RPC URL was not provided")
            .clone();

        self.svm_rpc
            .get_or_try_init(|| async { Ok(Arc::new(SolanaRpc::new(url))) })
            .await
    }
}
