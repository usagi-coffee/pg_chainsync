pub type EvmPubSub =
    alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend>;
pub type EvmPubSubError =
    alloy::transports::RpcError<alloy::transports::TransportErrorKind>;

pub type EvmLogResponse = alloy::rpc::types::Log;

pub type EvmBlock = alloy::rpc::types::Header;
pub type EvmLog = alloy::rpc::types::Log;

use crate::types::Job;

impl Job {
    pub async fn connect_evm(
        &self,
    ) -> anyhow::Result<&EvmPubSub, EvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");

        self.evm
            .get_or_try_init(|| async {
                let ws = alloy::providers::WsConnect::new(url);
                alloy::providers::ProviderBuilder::new().on_ws(ws).await
            })
            .await
    }

    pub async fn reconnect_evm(
        &self,
    ) -> anyhow::Result<EvmPubSub, EvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");

        let ws = alloy::providers::WsConnect::new(url);
        alloy::providers::ProviderBuilder::new().on_ws(ws).await
    }
}
