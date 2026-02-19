use std::sync::{OnceLock, RwLock};

use crate::types::HandlerRuntime;
use tokio::sync::OnceCell;

static HANDLERS: OnceLock<RwLock<Vec<HandlerRuntime>>> = OnceLock::new();

fn handlers_store() -> &'static RwLock<Vec<HandlerRuntime>> {
    HANDLERS.get_or_init(|| RwLock::new(Vec::new()))
}

impl HandlerRuntime {
    pub fn replace_all(mut handlers: Vec<HandlerRuntime>) {
        for handler in &mut handlers {
            handler.evm = OnceCell::const_new();
            handler.svm_rpc = OnceCell::const_new();
            handler.svm_ws = OnceCell::const_new();
        }

        let mut guard = handlers_store().write().expect("handlers store write");
        *guard = handlers;
    }

    pub fn query_all() -> Result<Vec<HandlerRuntime>, anyhow::Error> {
        Ok(handlers_store()
            .read()
            .expect("handlers store read")
            .clone())
    }
}
