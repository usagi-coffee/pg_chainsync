# pg_chainsync v2 Agent Guide

## Mission
Build and evolve `pg_chainsync` as a filesystem-first PGRX extension where handler logic is implemented in native Rust modules with a binary protocol and host-managed DB execution.

## Monorepo structure

- `crates/extension`: extension runtime and worker implementation
- `crates/evm`: shared EVM primitives and connection helpers
- `crates/svm`: shared SVM primitives and connection helpers
- `crates/channel`: shared channel primitives
- `crates/sdk`: safe plugin SDK and export macro
- `plugins/*`: example plugin crates

## Core architecture (must preserve)

- Handler definitions live in `<data_directory>/chainsync/handlers/*.toml`.
- Plugin binaries are loaded from `<data_directory>/chainsync/plugins/*.so`.
- Module workers execute business logic.
- A dedicated DB executor thread is the single SPI owner.
- Worker/DB communication is a typed internal bus.
- Runtime artifacts are filesystem data under `chainsync/logs` and `chainsync/state`.
- Shared ingress layer deduplicates upstream subscriptions and decoded events, then fans out to bound handlers.

## Hard constraints

- Do not introduce SQL status APIs (`list_jobs`, `job_status`, etc.).
- Do not allow arbitrary SQL from modules.
- Do not let modules call SPI directly.
- Do not break ABI compatibility rules without explicit ABI version bump/update.
- Keep unsafe FFI handling centralized in SDK/runtime glue, not in plugin business logic.
- Support `setup` hook only; do not rely on `cleanup` hooks.

## Public surface

### SQL
- Keep minimal SQL surface:
  - `chainsync.reload()`
  - `chainsync.restart()`
  - `chainsync.stop()`

### GUC
- `chainsync.database`

## Module ABI contract

Required exports:

- `chainsync_plugin_meta_v1`
- `chainsync_handle_event_v1`
- `chainsync_plugin_free_buffer`

Protocol requirements:

- Binary payloads only (prefer `rkyv`)
- Strict ABI major match at load
- Structured step protocol:
  - `NeedLookup`
  - `Done`
  - `Ignore`
  - `Error`

## Query and mutation model

- Queries are declared inline in handler TOML files (`sql_inline`).
- Host loads and validates query definitions on reload/startup.
- Host prepares plans and caches handles.
- Module references only stable IDs (`query_id`/`statement_id`) plus typed params.
- Host executes prepared plans transactionally.
- Host supports prelookup enrichment before first module invocation.

## Resume model

- Use `resume_token` to continue multi-step workflows after lookups.
- Avoid replaying full context payloads when tokenized context is sufficient.

## First-call enrichment model

- Support handler-configured `prelookups` resolved by host before first module call.
- Pass prefetched values and handler `state_path` in module input payload.
- Keep `NeedLookup` path available for misses or conditional secondary data.

## Lifecycle model

- `setup` runs on handler load/reload before processing events.
- Recovery logic must live in `setup` using handler runtime state files.
- `cleanup` is not part of the guaranteed lifecycle contract.

## Safety and reliability rules

- One SPI owner thread only.
- All DB writes are host-side and allowlisted.
- Emit deterministic runtime artifacts (`logs/<handler_id>.log`, `state/<handler_id>.bin`).
- On config/plugin/ABI errors, fail closed for that handler and log reason.
- One slow/failing handler must not block ingress or sibling handlers (queue isolation/backpressure required).

## Performance rules

- Prefer binary schemas with stable versioning.
- Minimize lookup roundtrips.
- Batch mutations when possible.
- Use prepared plans for all repeated queries/mutations.
- Deduplicate identical subscriptions (same canonical source key) and decode once before fanout.

## Ingress dedup model

- Build canonical subscription key from chain/provider/address/event-filter dimensions.
- Maintain one active ingress stream per key.
- Maintain bindings from ingress key to handler IDs.
- Decode each upstream event once and fan out to all bound handlers.
- Preserve per-handler idempotency/checkpointing independently after fanout.

## Definition of done for changes

- Documentation updated if protocol/runtime behavior changes.
- `cargo fmt` + `cargo check` pass.
- No regressions in worker-thread/SPI-thread separation.
- Runtime artifacts remain handler-isolated and actionable.
