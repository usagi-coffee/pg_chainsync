use crate::types::Job;

pub use evm_core::{EvmBlock, EvmLog, EvmLogResponse, EvmPubSub, EvmPubSubError};

impl Job {
    pub async fn connect_evm(&self) -> anyhow::Result<&EvmPubSub, EvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");
        evm_core::connect_ws(&self.evm, url).await
    }

    pub async fn reconnect_evm(&self) -> anyhow::Result<EvmPubSub, EvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");
        evm_core::reconnect_ws(url).await
    }
}

pub mod blocks;
pub mod logs;
pub mod tasks;
