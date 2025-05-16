use std::sync::Arc;

use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
use solana_transaction_status_client_types::TransactionDetails;
use solana_transaction_status_client_types::UiConfirmedBlock;

pub type SvmPubSub = solana_client::nonblocking::pubsub_client::PubsubClient;
pub type SvmPubSubError = solana_client::pubsub_client::PubsubClientError;
pub type SvmRpc = solana_client::nonblocking::rpc_client::RpcClient;

pub type SvmBlock = UiConfirmedBlock;
pub type SvmLog = solana_client::rpc_response::Response<
    solana_client::rpc_response::RpcLogsResponse,
>;
pub type SvmTransaction = EncodedConfirmedTransactionWithStatusMeta;
pub type SvmTransactionDetails = TransactionDetails;

use crate::types::Job;

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
