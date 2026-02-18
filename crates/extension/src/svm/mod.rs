use std::sync::Arc;

use crate::types::HandlerRuntime;

pub use svm_core::{
    RawSvmTransaction, SvmAccount, SvmBlock, SvmInitializedAccount, SvmLog, SvmPubSub,
    SvmPubSubError, SvmRpc, SvmTransaction, SvmTransactionDetails,
};

impl HandlerRuntime {
    pub async fn connect_svm_ws(
        &self,
    ) -> anyhow::Result<&Arc<SvmPubSub>, SvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");
        svm_core::connect_ws(&self.svm_ws, url).await
    }

    pub async fn reconnect_svm_ws(
        &self,
    ) -> anyhow::Result<SvmPubSub, SvmPubSubError> {
        let url = self
            .options
            .ws
            .as_ref()
            .expect("Websocket URL was not provided");
        svm_core::reconnect_ws(url).await
    }

    pub async fn connect_svm_rpc(&self) -> anyhow::Result<&Arc<SvmRpc>> {
        let url = self
            .options
            .rpc
            .as_ref()
            .expect("RPC URL was not provided")
            .clone();
        svm_core::connect_rpc(&self.svm_rpc, url).await
    }

    pub async fn reconnect_svm_rpc(&self) -> SvmRpc {
        let url = self
            .options
            .rpc
            .as_ref()
            .expect("RPC URL was not provided")
            .clone();
        svm_core::reconnect_rpc(url)
    }
}

pub mod blocks;
pub mod logs;
