use crate::types::Job;
use alloy::{
    network::{AnyHeader, AnyNetwork},
    providers::Identity,
};

pub type EvmPubSub = alloy::providers::RootProvider<AnyNetwork>;
pub type EvmPubSubError =
    alloy::transports::RpcError<alloy::transports::TransportErrorKind>;

pub type EvmLogResponse = alloy::rpc::types::Log;

pub type EvmBlock = alloy::rpc::types::Header<AnyHeader>;
pub type EvmLog = alloy::rpc::types::Log;

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
                alloy::providers::ProviderBuilder::<
                    Identity,
                    Identity,
                    AnyNetwork,
                >::default()
                .connect_ws(alloy::providers::WsConnect::new(url))
                .await
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

        alloy::providers::ProviderBuilder::<
            Identity,
            Identity,
            AnyNetwork,
        >::default()
        .connect_ws(alloy::providers::WsConnect::new(url))
        .await
    }
}

pub mod blocks;
pub mod logs;
pub mod tasks;
