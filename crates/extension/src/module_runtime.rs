use anyhow::{Context, Result, bail};
use libloading::Library;
use serde_json::Value;

use crate::types::HandlerRuntime;

type HandleFn = unsafe extern "C" fn(
    input_ptr: *const u8,
    input_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32;
type SetupFn = unsafe extern "C" fn(
    input_ptr: *const u8,
    input_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32;
type FreeFn = unsafe extern "C" fn(ptr: *mut u8, len: usize);

fn handler_path(handler: &HandlerRuntime) -> Result<&str> {
    handler
        .options
        .plugin_path
        .as_ref()
        .map(String::as_str)
        .context("handler has no plugin_path in options")
}

pub fn invoke(handler: &HandlerRuntime, payload: &[u8]) -> Result<Vec<u8>> {
    let path = handler_path(handler)?;

    // SAFETY: ABI and symbol presence are validated during handler load, and outputs are copied before free.
    unsafe {
        let lib =
            Library::new(path).with_context(|| format!("loading {}", path))?;
        let handle: libloading::Symbol<HandleFn> = lib
            .get(b"chainsync_handle_event_v1\0")
            .context("missing chainsync_handle_event_v1")?;
        let free: libloading::Symbol<FreeFn> = lib
            .get(b"chainsync_plugin_free_buffer\0")
            .context("missing chainsync_plugin_free_buffer")?;

        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let code =
            handle(payload.as_ptr(), payload.len(), &mut out_ptr, &mut out_len);
        if code != 0 {
            bail!("module returned error code {}", code);
        }

        if out_ptr.is_null() || out_len == 0 {
            return Ok(Vec::new());
        }

        let out =
            std::slice::from_raw_parts(out_ptr as *const u8, out_len).to_vec();
        free(out_ptr, out_len);
        Ok(out)
    }
}

pub fn setup(handler: &HandlerRuntime, payload: &[u8]) -> Result<Value> {
    let path = handler_path(handler)?;

    // SAFETY: ABI and symbol presence are validated during handler load.
    unsafe {
        let lib =
            Library::new(path).with_context(|| format!("loading {}", path))?;
        let setup: libloading::Symbol<SetupFn> = lib
            .get(b"chainsync_setup_v1\0")
            .context("missing chainsync_setup_v1")?;

        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let code =
            setup(payload.as_ptr(), payload.len(), &mut out_ptr, &mut out_len);
        if code != 0 {
            bail!("module setup returned error code {}", code);
        }

        if out_ptr.is_null() || out_len == 0 {
            return Ok(Value::Null);
        }

        let free: libloading::Symbol<FreeFn> = lib
            .get(b"chainsync_plugin_free_buffer\0")
            .context("missing chainsync_plugin_free_buffer")?;
        let out =
            std::slice::from_raw_parts(out_ptr as *const u8, out_len).to_vec();
        free(out_ptr, out_len);
        let value: Value =
            serde_json::from_slice(&out).context("decoding setup response")?;
        Ok(value)
    }
}
