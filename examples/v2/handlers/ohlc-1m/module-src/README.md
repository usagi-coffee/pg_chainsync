# ohlc-1m module

Build the module and place it as `handler.so` in the handler directory.

## Build

```bash
cd examples/v2/handlers/ohlc-1m/module-src
cargo build --release
cp target/release/libohlc_handler.so ../handler.so
```

Or use:

```bash
./examples/v2/handlers/ohlc-1m/build_handler.sh
```

## State path

Module durable state path is injected by host in payload (`state_path`).
Env vars are optional overrides:

- `CHAINSYNC_STATE_PATH` (exact full path)
- or `CHAINSYNC_STATE_ROOT` (module will use `<root>/<job_id>_state.json`)
- default when none provided: `/tmp/ohlc_state_<job_id>.json`

Set it to your handler runtime state file, for example:

```bash
export CHAINSYNC_STATE_PATH=/etc/pg_chainsync/handlers/_runtime/ohlc-1m/state.bin
```
