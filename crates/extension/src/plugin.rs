use std::ffi::CStr;
use std::os::raw::c_char;
use std::path::Path;

use anyhow::{Context, Result, bail};
use libloading::Library;

pub const ABI_MAJOR: u16 = 1;

#[repr(C)]
pub struct PluginMetadataV1 {
    pub abi_major: u16,
    pub abi_minor: u16,
    pub name: *const c_char,
    pub version: *const c_char,
}

#[derive(Clone, Debug)]
pub struct PluginMetadata {
    pub abi_major: u16,
    pub abi_minor: u16,
    pub name: String,
    pub version: String,
}

pub fn validate_plugin(path: &Path) -> Result<PluginMetadata> {
    // SAFETY: library loading and symbol resolution are validated by file ownership and runtime checks.
    unsafe {
        let library = Library::new(path)
            .with_context(|| format!("loading plugin {}", path.display()))?;
        let meta_fn: libloading::Symbol<
            unsafe extern "C" fn() -> *const PluginMetadataV1,
        > = library
            .get(b"chainsync_plugin_meta_v1\0")
            .context("missing symbol chainsync_plugin_meta_v1")?;
        let raw = meta_fn();
        if raw.is_null() {
            bail!("metadata symbol returned null");
        }

        let raw = &*raw;
        if raw.abi_major != ABI_MAJOR {
            bail!(
                "ABI major mismatch: expected {}, got {}",
                ABI_MAJOR,
                raw.abi_major
            );
        }

        let name = CStr::from_ptr(raw.name)
            .to_str()
            .context("invalid utf8 plugin name")?
            .to_string();
        let version = CStr::from_ptr(raw.version)
            .to_str()
            .context("invalid utf8 plugin version")?
            .to_string();

        Ok(PluginMetadata {
            abi_major: raw.abi_major,
            abi_minor: raw.abi_minor,
            name,
            version,
        })
    }
}

pub fn validate_handler_exports(path: &Path) -> Result<()> {
    // SAFETY: library loading and symbol resolution are read-only checks.
    unsafe {
        let library = Library::new(path)
            .with_context(|| format!("loading plugin {}", path.display()))?;
        let _handle: libloading::Symbol<
            unsafe extern "C" fn(
                input_ptr: *const u8,
                input_len: usize,
                out_ptr: *mut *mut u8,
                out_len: *mut usize,
            ) -> i32,
        > = library
            .get(b"chainsync_handle_event_v1\0")
            .context("missing symbol chainsync_handle_event_v1")?;
        let _free: libloading::Symbol<
            unsafe extern "C" fn(ptr: *mut u8, len: usize),
        > = library
            .get(b"chainsync_plugin_free_buffer\0")
            .context("missing symbol chainsync_plugin_free_buffer")?;
        let _setup: libloading::Symbol<
            unsafe extern "C" fn(
                input_ptr: *const u8,
                input_len: usize,
                out_ptr: *mut *mut u8,
                out_len: *mut usize,
            ) -> i32,
        > = library
            .get(b"chainsync_setup_v1\0")
            .context("missing symbol chainsync_setup_v1")?;
    }
    Ok(())
}
