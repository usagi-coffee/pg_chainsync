# erc20-transfer plugin

SDK-based EVM log handler for `Transfer(address,address,uint256)`.

## Files

- `schema.sql`: suggested table/index bootstrap

## Build

```bash
cargo build --release -p erc20_transfer_handler
```

Then deploy resulting `liberc20_transfer_handler.so` to:

- `<data_directory>/chainsync/plugins/erc20-transfer.so`

and place a handler instance TOML in:

- `<data_directory>/chainsync/handlers/<handler_id>.toml`

then call:

```sql
SELECT chainsync.reload();
```
