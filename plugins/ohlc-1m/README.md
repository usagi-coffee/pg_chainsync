# ohlc-1m plugin

Build the plugin and copy the resulting `.so` into `<data_directory>/chainsync/plugins`.

## Build

```bash
cd plugins/ohlc-1m
cargo build --release
cp ../../target/release/libohlc_handler.so <data_directory>/chainsync/plugins/ohlc-1m.so
```

Handler instance TOML lives separately in:

- `<data_directory>/chainsync/handlers/<handler_id>.toml`
