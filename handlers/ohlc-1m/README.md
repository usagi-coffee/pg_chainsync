# ohlc-1m module

Build the module and copy the resulting `.so` into `chainsync.config_dir` (SO-only mode).

## Build

```bash
cd handlers/ohlc-1m
cargo build --release
cp ../../target/release/libohlc_handler.so /path/to/handlers/ohlc-1m.so
```

## State path

Module durable state path is injected by host in payload (`state_path`).
Env vars are optional overrides:

- `CHAINSYNC_STATE_PATH` (exact full path)
- or `CHAINSYNC_STATE_ROOT` (module will use `<root>/<handler_id>_state.json`)
- default when none provided: `/tmp/ohlc_state_<handler_id>.json`

Set it to your handler runtime state file, for example:

```bash
export CHAINSYNC_STATE_PATH=/etc/pg_chainsync/handlers/_runtime/ohlc-1m/state.bin
```
