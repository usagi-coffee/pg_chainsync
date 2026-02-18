use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use libloading::Library;

use crate::types::Job;

type HandleFn = unsafe extern "C" fn(
    input_ptr: *const u8,
    input_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32;
type FreeFn = unsafe extern "C" fn(ptr: *mut u8, len: usize);

fn handler_path(job: &Job) -> Result<PathBuf> {
    let base = job
        .options
        .handler_dir
        .as_ref()
        .context("job has no handler_dir in options")?;
    Ok(PathBuf::from(base).join("handler.so"))
}

pub fn invoke(job: &Job, payload: &[u8]) -> Result<Vec<u8>> {
    let path = handler_path(job)?;

    // SAFETY: ABI and symbol presence are validated during handler load, and outputs are copied before free.
    unsafe {
        let lib = Library::new(&path)
            .with_context(|| format!("loading {}", path.display()))?;
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
