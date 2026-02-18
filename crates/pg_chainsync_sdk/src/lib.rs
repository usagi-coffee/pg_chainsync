use std::ffi::c_char;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[repr(C)]
pub struct PluginMetadataV1 {
    pub abi_major: u16,
    pub abi_minor: u16,
    pub name: *const c_char,
    pub version: *const c_char,
}

unsafe impl Sync for PluginMetadataV1 {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModuleResponse {
    Ignore,
    Error { message: String },
    Done { mutations: Vec<Mutation> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mutation {
    pub id: String,
    pub payload: Value,
}

pub trait PluginHandler {
    fn handle_event(input: Value) -> Result<ModuleResponse, String>;
}

#[macro_export]
macro_rules! export_plugin {
    ($handler:ty, $name:literal, $version:literal) => {
        static __CHAINSYNC_NAME: &[u8] = concat!($name, "\0").as_bytes();
        static __CHAINSYNC_VERSION: &[u8] =
            concat!($version, "\0").as_bytes();

        #[no_mangle]
        pub extern "C" fn chainsync_plugin_meta_v1(
        ) -> *const $crate::PluginMetadataV1 {
            static META: $crate::PluginMetadataV1 = $crate::PluginMetadataV1 {
                abi_major: 1,
                abi_minor: 0,
                name: __CHAINSYNC_NAME.as_ptr() as *const ::std::ffi::c_char,
                version: __CHAINSYNC_VERSION.as_ptr()
                    as *const ::std::ffi::c_char,
            };
            &META
        }

        #[no_mangle]
        pub extern "C" fn chainsync_handle_event_v1(
            input_ptr: *const u8,
            input_len: usize,
            out_ptr: *mut *mut u8,
            out_len: *mut usize,
        ) -> i32 {
            if input_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
                return 1;
            }

            let input_bytes = unsafe {
                ::std::slice::from_raw_parts(input_ptr, input_len)
            };
            let output: $crate::ModuleResponse = match ::serde_json::from_slice::<
                ::serde_json::Value,
            >(input_bytes)
            {
                Ok(input) => match <$handler as $crate::PluginHandler>::handle_event(input) {
                    Ok(output) => output,
                    Err(error) => $crate::ModuleResponse::Error {
                        message: error,
                    },
                },
                Err(error) => $crate::ModuleResponse::Error {
                    message: error.to_string(),
                },
            };

            let encoded = match ::serde_json::to_vec(&output) {
                Ok(v) => v,
                Err(_) => return 2,
            };

            let mut boxed = encoded.into_boxed_slice();
            let ptr = boxed.as_mut_ptr();
            let len = boxed.len();
            ::std::mem::forget(boxed);

            unsafe {
                *out_ptr = ptr;
                *out_len = len;
            }

            0
        }

        #[no_mangle]
        pub extern "C" fn chainsync_plugin_free_buffer(ptr: *mut u8, len: usize) {
            if ptr.is_null() || len == 0 {
                return;
            }

            unsafe {
                let _ = ::std::vec::Vec::from_raw_parts(ptr, len, len);
            }
        }
    };
}
