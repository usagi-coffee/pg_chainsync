# v2 examples

## OHLC handler

Files:

- `handlers/ohlc-1m/handler.toml`
- `handlers/ohlc-1m/queries/upsert_ohlc_1m.sql`
- `handlers/ohlc-1m/module-src` (Rust `cdylib` source)
- `ohlc_schema.sql`
- `run_ohlc.sql`

## Setup

1. Copy `examples/v2/handlers/ohlc-1m` into your `chainsync.config_dir`.
2. Build module:
   - `./handlers/ohlc-1m/build_handler.sh`
   - Optional local module test: `cd handlers/ohlc-1m/module-src && cargo test`
3. Set env vars used by handler config:
   - `EVM_WS_URL`
   - `POOL_ADDRESS`
4. Optional override for module state path:
   - `CHAINSYNC_STATE_PATH` (exact path) or
   - `CHAINSYNC_STATE_ROOT` (directory root)
   - By default host injects `state_path` as `<config_dir>/_runtime/<handler_id>/state.bin`.
5. Run:

```sql
\i examples/v2/run_ohlc.sql
```

or from shell:

```bash
./examples/v2/run_ohlc.sh
```

6. Seed pool metadata used by prelookup:

```sql
INSERT INTO token_pool_meta(pool_address, base_decimals, quote_decimals)
VALUES ('0x...', 6, 6)
ON CONFLICT (pool_address) DO UPDATE
SET base_decimals = EXCLUDED.base_decimals,
    quote_decimals = EXCLUDED.quote_decimals;
```

The module keeps active candle state in durable state file and emits DB mutation only when minute bucket is finalized.

Check results:

```sql
SELECT pair_id, bucket_start, open, high, low, close, trades
FROM ohlc_1m
ORDER BY bucket_start DESC
LIMIT 20;
```
