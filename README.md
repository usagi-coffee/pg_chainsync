# pg_chainsync v2

`pg_chainsync` v2 is a PGRX extension for blockchain sync in PostgreSQL with filesystem-native handler configs, handler-local Rust modules, binary module ABI, and host-managed database execution.

## Architecture at a glance

- Handlers are TOML files in `chainsync.config_dir`.
- Modules are Rust `.so` files colocated with each handler directory.
- Event workers run module logic.
- A dedicated DB executor thread owns SPI execution.
- Workers and DB executor communicate over an internal typed bus.
- Each handler has isolated runtime artifacts in its own directory.

## Monorepo layout

- `crates/pg_chainsync`: PGRX extension crate
- `crates/evm`: internal EVM primitives crate
- `crates/svm`: internal SVM primitives crate
- `crates/channel`: internal channel primitives crate
- `crates/pg_chainsync_sdk`: plugin SDK crate (`export_plugin!`, response types)
- `handlers/ohlc-1m`: example handler crate

## Design decisions

- No SQL status APIs such as `list_jobs`/`job_status`.
- Runtime state and logs are filesystem artifacts per handler.
- Binary module payloads only (no JSON event ABI).
- Plugins do not execute arbitrary SQL.
- SQL is executed by host-side Rust via SPI from allowlisted query registry.
- Queries are prepared/cached as plans at runtime (`reload`/startup).
- Lifecycle hook policy: `setup` only (no `cleanup` hook guarantee).
- Host performs optional prelookup enrichment before first module call.
- Shared ingress deduplicates subscriptions/decode and fans out one event to many handlers.

## Requirements

- PostgreSQL 17
- Rust toolchain (see `rust-toolchain.toml`)
- `cargo-pgrx`

## Build extension

```bash
cargo install --locked cargo-pgrx
cargo build --release -p pg_chainsync
cargo pgrx package --manifest-path crates/pg_chainsync/Cargo.toml
```

Copy extension artifacts according to your `pg_config` installation paths.

## PostgreSQL configuration

```conf
shared_preload_libraries = 'pg_chainsync'

chainsync.database = 'postgres'
# optional, defaults to <data_directory>/chainsync/handlers
# chainsync.config_dir = '/etc/pg_chainsync/handlers'
```

Restart PostgreSQL after config changes.

## SQL surface

```sql
CREATE EXTENSION pg_chainsync;

SELECT chainsync.restart();
SELECT chainsync.stop();
SELECT chainsync.reload();
```

## Filesystem layout

```text
/etc/pg_chainsync/
  handlers/
    evm-transfer-stream/
      handler.toml
      handler.so
      queries/
        token_meta_by_address.sql
        upsert_transfer.sql
    svm-program-cron/
      handler.toml
      handler.so
      queries/
        ...
    _runtime/
      evm-transfer-stream/
        status.json
        logs/
          loader.log
      svm-program-cron/
        status.json
        logs/
          loader.log
```

## Handler format

Each handler directory contains one `handler.toml`.

### Required keys

- `[handler].id`
- `[handler].chain` = `"evm" | "svm"`
- `[handler].mode` = `"stream" | "oneshot" | "cron"`
- Module file `handler.so` must exist in the same handler directory

### Optional keys

- top-level: `rpc`, `ws`, `preload`, `oneshot`, `cron`, `setup_handler`, `success_handler`, `failure_handler`

### Validation

- `handler.chain = "evm"` requires `[evm]`
- `handler.chain = "svm"` requires `[svm]`
- `handler.mode = "cron"` requires `cron`
- `${ENV_VAR}` placeholders are resolved from process environment

### Example handler TOML

```toml
[handler]
id = "evm-transfer-stream"
chain = "evm"
mode = "stream"

ws = "${EVM_WS_URL}"

[evm]
address = "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
event = "Transfer(address,address,uint256)"

[queries.lookups.token_meta_by_address]
sql = "queries/token_meta_by_address.sql"

[queries.mutations.upsert_transfer]
sql = "queries/upsert_transfer.sql"

[runtime]
prelookups = ["token_meta_by_address"]
```

## Runtime artifacts per handler

- `<config_dir>/_runtime/<job_id>/status.json`
- `<config_dir>/_runtime/<job_id>/logs/loader.log`

`loader.log` is JSONL and records states (`LOADED`, `ERROR`, `REMOVED`) and reload/validation events.

Handler setup state is stored under:

- `<config_dir>/_runtime/<job_id>/state.bin`

`setup` initializes/recovers this state before event processing.

## Module ABI (binary)

Required export:

- `chainsync_plugin_meta_v1`

Recommended event handler contract:

```rust
#[no_mangle]
pub extern "C" fn chainsync_handle_event_v1(
    input_ptr: *const u8,
    input_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32
```

Required buffer free function:

```rust
#[no_mangle]
pub extern "C" fn chainsync_plugin_free_buffer(ptr: *mut u8, len: usize)
```

- Use binary serialization (`rkyv` recommended).
- Host enforces strict ABI major version match.

## Safe plugin development via SDK

Module authors should use a plugin SDK crate that provides:

- safe `PluginHandler` trait
- macro to export FFI symbols
- centralized unsafe pointer handling
- binary decode/encode + panic boundary handling

Plugin crates should only implement typed business logic.

## Handler lifecycle hooks

- `setup` is supported and runs when handler is loaded/reloaded before processing events.
- `cleanup` is intentionally not part of the contract (not reliable on crashes/forced stop).
- Any recovery logic must be handled by `setup` using handler runtime state files.

## Host/module protocol

The protocol is multi-step and typed.

### Input side

- `Event { event, prefetched }`
- `Resume { resume_token, lookup_result }`

### Output side

- `NeedLookup { query_id, params, resume_token }`
- `Done { idempotency_key, mutations }`
- `Ignore`
- `Error { message }`

## Shared ingress + fanout (event dedup for many handlers)

Handlers with the same source filters share one upstream ingress stream.

Canonical subscription key includes:

- chain
- provider/ws endpoint
- address
- event signature / topic0
- any additional filter dimensions used by the listener

Runtime behavior:

1. Create one upstream subscription per canonical key.
2. Decode event once into one canonical envelope.
3. Fan out that event to all bound handlers for that key.
4. Each handler runs independently (state, checkpoints, mutations).

Notes:

- This deduplicates network subscription and decode cost, not handler side effects.
- Handlers remain isolated; one handler falling behind must not block others.
- Per-handler queues/backpressure should be enforced at dispatch.

## Database access model

Plugins do not run SQL directly.

### Query path

1. Module emits `NeedLookup { query_id, params, resume_token }`.
2. DB executor thread resolves `query_id` in handler query registry.
3. DB executor executes prepared SPI plan with typed params.
4. DB executor returns `lookup_result` to module with `resume_token`.

## Prelookup enrichment (first-call context)

Handlers can declare prelookups that the host resolves before the first module call for each event.

Typical usage:

- enrich EVM log event with token decimals/symbol
- enrich account event with owner/mint metadata

Flow:

1. Event arrives.
2. Host runs handler-configured prelookup query IDs via prepared SPI plans.
3. Host caches prelookup results per handler worker process.
4. Host builds event payload with `prefetched` and `state_path`.
5. Module receives enriched first call and can skip extra lookup roundtrip.
6. Module may still emit `NeedLookup` for cache misses or secondary data.

### Mutation path

1. Module emits `Done { mutations }`.
2. DB executor applies allowlisted mutations via prepared SPI plans in transaction.
3. DB executor handles checkpoint/idempotency policy.

## Queries and prepared plans

Each handler defines lookup/mutation IDs directly in `handler.toml`.

At reload/startup:

- host loads query definitions from `handler.toml`
- resolves `${ENV_VAR}` placeholders in SQL files
- validates IDs/files
- prepares plans (`SPI_prepare`) and caches handles

At runtime:

- host executes prepared plans (`SPI_execute_plan`) only
- no arbitrary SQL strings from plugin responses

## Real-world pattern: `erc20_transfer_ingestor`

Typical flow:

1. Event arrives for ERC-20 transfer.
2. Module decodes topics/data and emits `NeedLookup(token_meta_by_address)`.
3. DB executor returns token decimals.
4. Module emits `Done` with `upsert_transfer` mutation + idempotency key.
5. DB executor writes transactionally.

## Performance notes

- Binary payloads reduce serialization overhead.
- Prepared plans reduce parse/plan overhead.
- Throughput depends on minimizing lookup roundtrips and batching mutations.
- For DB-heavy workloads, performance is usually near PL/pgSQL; CPU-heavy logic benefits more from Rust modules.

## Operational flow

1. Create/update handler directory (`handler.toml` + `queries/*`).
2. Deploy module `.so`.
3. Run `SELECT chainsync.reload();`.
4. Check `_runtime/<job_id>/status.json` and `logs/loader.log`.
5. Restart worker if needed.

## Development

```bash
cargo fmt --all
cargo check
```

## Handler developer workflow

Use the helper script for a fast build-deploy-reload loop:

```bash
bun run scripts/dev-handler.ts <cargo_package> <handler_id>
```

Example:

```bash
bun run scripts/dev-handler.ts ohlc_handler ohlc-1m
```

What it does:

1. Builds the handler crate.
2. Copies `lib*.so` to `<handlers_dir>/<handler_id>/handler.so`.
3. Executes `SELECT chainsync.reload();` through `psql`.

Defaults:

- `CHAINSYNC_HANDLERS_DIR`:
  if unset, script resolves `SHOW data_directory` and uses `<data_directory>/chainsync/handlers`
- `PGURL=postgresql:///postgres`

Flags:

- `--debug` build debug profile instead of release
- `--no-reload` skip database reload

This keeps extension restarts out of the hot path while iterating on handler logic.

## License

MIT
