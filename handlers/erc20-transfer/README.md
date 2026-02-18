# erc20-transfer handler

SDK-based EVM log handler for `Transfer(address,address,uint256)`.

## Files

- `handler.toml`: embedded handler config consumed by the extension
- `schema.sql`: suggested table/index bootstrap

## Build

```bash
cargo build --release -p erc20_transfer_handler
```

Then deploy resulting `liberc20_transfer_handler.so` into `chainsync.config_dir`, for example as `erc20-transfer.so`, and call:

```sql
SELECT chainsync.reload();
```
