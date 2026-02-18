use alloy::{
    network::{AnyHeader, AnyNetwork},
    providers::Identity,
};
use tokio::sync::OnceCell;

pub type EvmPubSub = alloy::providers::RootProvider<AnyNetwork>;
pub type EvmPubSubError =
    alloy::transports::RpcError<alloy::transports::TransportErrorKind>;

pub type EvmLogResponse = alloy::rpc::types::Log;

pub type EvmBlock = alloy::rpc::types::Header<AnyHeader>;
pub type EvmLog = alloy::rpc::types::Log;

pub async fn connect_ws<'a>(
    cell: &'a OnceCell<EvmPubSub>,
    url: &str,
) -> anyhow::Result<&'a EvmPubSub, EvmPubSubError> {
    cell.get_or_try_init(|| async {
        alloy::providers::ProviderBuilder::<Identity, Identity, AnyNetwork>::default()
            .connect_ws(alloy::providers::WsConnect::new(url))
            .await
    })
    .await
}

pub async fn reconnect_ws(url: &str) -> anyhow::Result<EvmPubSub, EvmPubSubError> {
    alloy::providers::ProviderBuilder::<Identity, Identity, AnyNetwork>::default()
        .connect_ws(alloy::providers::WsConnect::new(url))
        .await
}
