# pg_chainsync v2

`pg_chainsync` v2 is a PGRX extension for blockchain sync in PostgreSQL with filesystem-loaded Rust plugins (`.so`), filesystem handler TOML instances, binary module ABI, and host-managed database execution.

## Architecture at a glance

- Handlers are discovered from `*.toml` files in `<data_directory>/chainsync/handlers`.
- Plugins are discovered from `*.so` files in `<data_directory>/chainsync/plugins`.
- Each handler references a plugin via `[handler].plugin`.
- Event workers run module logic.
- Host runtime executes SPI operations inside PostgreSQL transaction boundaries.
- Runtime artifacts are stored under `chainsync/logs` and `chainsync/state`.

## Monorepo layout

- `crates/extension`: PGRX extension crate
- `crates/evm`: internal EVM primitives crate
- `crates/svm`: internal SVM primitives crate
- `crates/channel`: internal channel primitives crate
- `crates/sdk`: plugin SDK crate (`export_plugin!`, response types)
- `plugins/ohlc-1m`: example plugin crate

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

- PostgreSQL 18 (recommended)
- Rust toolchain (see `rust-toolchain.toml`)
- `cargo-pgrx`

## Build extension

```bash
cargo install --locked cargo-pgrx
cargo build --release -p pg_chainsync
cargo pgrx package --manifest-path crates/extension/Cargo.toml
```

Copy extension artifacts according to your `pg_config` installation paths.

## PostgreSQL configuration

```conf
shared_preload_libraries = 'pg_chainsync'

chainsync.database = 'postgres'
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
<data_directory>/
  chainsync/
    handlers/
      evm-transfer-mainnet.toml
      evm-transfer-base.toml
    plugins/
      erc20-transfer.so
      ohlc-1m.so
    logs/
      evm-transfer-mainnet.log
      evm-transfer-base.log
    state/
      evm-transfer-mainnet.bin
      evm-transfer-base.bin
```

## Handler format

`handler.toml` is loaded from `<data_directory>/chainsync/handlers/*.toml`.

### Required keys

- `[handler].id`
- `[handler].plugin`
- `[handler].chain` = `"evm" | "svm"`
- `[handler].mode` = `"stream"`
- Plugin file is `<data_directory>/chainsync/plugins/<handler.plugin>.so`

### Optional keys

- top-level: `rpc`, `ws`

### Validation

- `handler.chain = "evm"` requires `[evm]`
- `handler.chain = "svm"` requires `[svm]`
- `handler.mode` must be `"stream"` in v2
- `${ENV_VAR}` placeholders are resolved from process environment

### Example handler TOML

```toml
[handler]
id = "evm-transfer-stream"
plugin = "erc20-transfer"
chain = "evm"
mode = "stream"

ws = "${EVM_WS_URL}"

[evm]
address = "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
event = "Transfer(address,address,uint256)"

[queries.mutations.upsert_transfer]
sql_inline = """
INSERT INTO erc20_transfers (contract, tx_hash, log_index, amount_raw)
VALUES (($1->>'contract')::text, ($1->>'tx_hash')::text, ($1->>'log_index')::bigint, ($1->>'amount_raw')::numeric)
ON CONFLICT (contract, tx_hash, log_index) DO UPDATE
SET amount_raw = EXCLUDED.amount_raw;
"""

[runtime]
prelookups = []
```

In file-based handler mode, queries must use `sql_inline` (no external `queries/*.sql` files).

## Runtime artifacts per handler

- `<data_directory>/chainsync/logs/<handler_id>.log`

`<handler_id>.log` is JSONL and records states (`REGISTERED`, `UPDATED`, `ERROR`, `REMOVED`) and reload/validation events.

Handler setup state is stored under:

- `<data_directory>/chainsync/state/<handler_id>.bin`

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

Current plugin response model:

- `Done { mutations }`
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
Host executes allowlisted lookup/mutation queries declared in handler TOML.

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
6. Module uses `prefetched` values directly when available.

### Mutation path

1. Module emits `Done { mutations }`.
2. DB executor applies allowlisted mutations via prepared SPI plans in transaction.
3. DB executor handles checkpoint/idempotency policy.

## Queries and prepared plans

Each handler defines lookup/mutation IDs directly in file-based `handler.toml`.

At reload/startup:

- host loads query definitions from `handler.toml`
- resolves `${ENV_VAR}` placeholders in inline SQL
- validates IDs
- prepares plans (`SPI_prepare`) and caches handles

At runtime:

- host executes prepared plans (`SPI_execute_plan`) only
- no arbitrary SQL strings from plugin responses

## Real-world pattern: `erc20_transfer_ingestor`

Typical flow:

1. Event arrives for ERC-20 transfer.
2. Host enriches payload with prelookup results (for example recovery checkpoint).
3. Module decodes topics/data and emits `Done` with `upsert_transfer`.
4. Host applies mutation query transactionally.

## Performance notes

- Binary payloads reduce serialization overhead.
- Prepared plans reduce parse/plan overhead.
- Throughput depends on minimizing lookup roundtrips and batching mutations.
- For DB-heavy workloads, performance is usually near PL/pgSQL; CPU-heavy logic benefits more from Rust modules.

## Operational flow

1. Build plugin `.so`.
2. Copy plugin `.so` into `<data_directory>/chainsync/plugins` and handler TOML into `<data_directory>/chainsync/handlers`.
3. Run `SELECT chainsync.reload();`.
4. Check `chainsync/logs/<handler_id>.log`.
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

1. Builds the plugin crate.
2. Copies `lib*.so` to `<chainsync_dir>/plugins/<plugin_name>.so`.
3. Copies source `handler.toml` to `<chainsync_dir>/handlers/<handler_id>.toml`.
4. Executes `SELECT chainsync.reload();` through `psql`.

Defaults:

- `CHAINSYNC_DIR`:
  if unset, script resolves `SHOW data_directory` and uses `<data_directory>/chainsync`
- `PGURL=postgresql:///postgres`

Flags:

- `--debug` build debug profile instead of release
- `--no-reload` skip database reload

This keeps extension restarts out of the hot path while iterating on handler logic.

## License

MIT
