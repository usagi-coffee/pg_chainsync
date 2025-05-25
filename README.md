# pg_chainsync: access blockchain inside PostgreSQL

> Proof of Concept - expect bugs and breaking changes.

pg_chainsync adds ability to access blockchain blocks, events and more directly inside your PostgreSQL instance. The extension does not enforce any custom schema for your table and let's you use custom handlers that you adjust for your specific use-case.

The extension is created with [pgrx](https://github.com/tcdi/pgrx)

## Usage

```sql
CREATE EXTENSION pg_chainsync;
```

### Worker lifecycle

```sql
-- Restart your worker on-demand
SELECT chainsync.restart();

-- Stops the worker
SELECT chainsync.stop();
```

### Watching new blocks

> This scenario assumes there exists blocks table with number and hash column

```sql
-- This is your custom handler that inserts new blocks to your table
CREATE FUNCTION custom_block_handler(block chainsync.EvmBlock, job JSONB) RETURNS your_blocks
AS $$
INSERT INTO your_blocks (number, hash)
VALUES (block.number, block.hash)
RETURNING *
$$
LANGUAGE SQL;

-- Register a new job that will watch new blocks
SELECT chainsync.register(
  'simple-blocks',
  '{
    "ws": "wss://provider-url",
    "evm": {
      "block_handler": "custom_block_handler"
    }
  }'::JSONB);
```

For the optimal performance your handler function should meet the conditions to be [inlined](https://wiki.postgresql.org/wiki/Inlining_of_SQL_functions).

Here is the complete log output, for the testing the number of fetched blocks has been limited to display the full lifecycle.

![example_output](./extra/usage1.png)

### Watching new events

```sql

-- This is your custom handler that inserts events to your table
CREATE FUNCTION custom_log_handler(log chainsync.EvmLog, job JSONB) RETURNS your_logs
AS $$
INSERT INTO your_logs (address, data) -- Inserting into your custom table
VALUES (log.address, log.data)
RETURNING *
$$
LANGUAGE SQL;

SELECT chainsync.register(
  'custom-events',
  '{
    "ws": "ws://provider-url",
    "evm": {
      "log_handler": "custom_log_handler",
      "address": "0x....",
      "event": "Transfer(address,address,uint256)"
    }
  }'::JSONB
);

-- Optional: Restart worker (or entire database)
SELECT chainsync.restart();
```

### Oneshot tasks

Oneshot Task is a type of job that is designed to run only once or manually triggered.

Running this query will add a task that will fetch all transfer events for specific contract at address starting from block 12345 and fetching 10000 blocks per call once.

> Hint: Most providers limit the number of events/range of blocks returned from getLogs method so it will just fail, in this case you can use blocktick option that splits fetching into multiple calls, blocktick means range of blocks per call. This does not apply to watching events because they start from latest block.

```sql
SELECT chainsync.register(
  'oneshot-task',
  '{
    "ws": "ws://provider-url",
    "oneshot": true,
    "evm": {
      "log_handler": "custom_log_handler",
      "address": "0x....",
      "event": "Transfer(address,address,uint256)",
      "from_block": 12345,
      "blocktick": 10000
    }
  }'::JSONB
);

```

#### Cron tasks

Cron tasks are supported, simply add `cron` key to your configuration json.

> Hint: cron expression value should be 6 characters because it supports seconds resolution e.g `0 * * * * *` - will run every minute

```sql
SELECT chainsync.register(
  'transfers-every-minute',
  '{
    "ws": "wss://provider-url",
    "cron": "0 * * * * *",
    "evm": {
      "log_handler": "transfer_handler",
      "address": "0x....",
      "event": "Transfer(address,address,uint256)",
      "from_block": 0
    }
  }'::JSONB
);
```

#### Preloaded tasks

Some tasks need to be run when the database starts, for that you can use `preload_events_task`, the created task will run when the extension or the database re/starts.

```sql
SELECT chainsync.register(
  'transfers-on-restart',
  '{
    "ws": "wss://provider-url",
    "preload": true,
    "evm": {
      "log_handler": "transfer_handler",
      "address": "0x....",
      "event": "Transfer(address,address,uint256)",
      "from_block": 0
    }
  }'::JSONB
);
```

#### Handle blocks before events

`await_block` is a feature that allows you to fetch and handle event's block before handling the event. This is helpful when you want to e.g join block inside your event handler, this ensures there is always block available for your specific event when you call your event handler.

You can optionally skip block fetching and handling if you specify `block_lookup` property which is the name of the function that takes `(block BIGINT, job JSONB)` and returns any value - if it returns any value then it will skip handling this block.

```sql
-- Look for block in your schemas and return e.g block number
CREATE FUNCTION find_block(block BIGINT, job JSONB) RETURNS BIGINT
AS $$
SELECT block_column FROM your_blocks
WHERE chain_column = job->>'your_custom_property' AND block_column = block
LIMIT 1
$$ LANGUAGE SQL;

SELECT chainsync.register(
  'ensure-blocks',
  '{
    "ws": "wss://provider-url",
    "evm": {
      "log_handler": "transfer_handler",
      "address": "0x....",
      "event": "Transfer(address,address,uint256)",

      "await_block": true,
      "block_skip_lookup": "find_block",
      "block_handler": "insert_block",
    },
    "your_custom_property": 31337
  }'::JSONB
);

```

## Installation

```bash
# Install pgrx
cargo install --locked cargo-pgrx

# Build the extension
cargo build --release

# Packaging process should create pg_chainsync-pg.. under target/release
cargo pgrx package

# NOTICE: your paths may be different because of pg_config... adjust them accordingly to your host/target machine
cp target/release/pg_chainsync-.../.../pg_chainsync.so /usr/lib/postgresql/
cp target/release/pg_chainsync-.../.../pg_chainsync--....sql /usr/share/postgresql/extension/
cp target/release/pg_chainsync-.../.../pg_chainsync.control /usr/share/postgresql/extension/
```

This should be enough to be able to use `CREATE EXTENSION pg_chainsync` but we also need to preload our extension because it uses background worker, to preload the extension you need to modify the `postgresql.conf` file and alter `shared_preload_libraries`

```
shared_preload_libraries = 'pg_chainsync.so' # (change requires restart)
```

After adjusting the config, restart your database and you can check postgres logs to check if it worked!

> Please refer to the pgrx documentation for full details on how to install background worker extension if it does not work for you

## Examples

You can check out how the extension works in action with `podman compose` (podman) or `docker compose` (docker), you can run the examples using the `dev.sh` script e.g `./dev.sh examples/demo.sql`.

```bash
bun run demo # Runs examples/demo.sql
```

Currently the extension is built on the host machine so keep in mind your paths may vary depending on your `pg_config`, make sure the extension gets built into the correct path, if it's different you need to adjust the volumes in `docker-compose.yml` file, here is how you need to adjust them.

```yaml
- ./target/release/pg_chainsync-pg17/usr/lib64/pgsql/pg_chainsync.so:/usr/lib/postgresql/17/lib/pg_chainsync.so:z
- ./target/release/pg_chainsync-pg17/usr/share/pgsql/extension/pg_chainsync.control:/usr/share/postgresql/17/extension/pg_chainsync.control:z
- ./target/release/pg_chainsync-pg17/usr/share/pgsql/extension/pg_chainsync--0.0.0.sql:/usr/share/postgresql/17/extension/pg_chainsync--0.0.0.sql:z
```

## Configuration

The extension is configurable through `postgresql.conf` file, here are the supported keys that you can modify.

| GUC Variable                  | Description                                                     | Default  |
| ----------------------------- | --------------------------------------------------------------- | -------- |
| chainsync.database            | Database name the extension will run on                         | postgres |
| chainsync.evm_ws_permits      | Number of concurrent tasks that can run using the same provider | 1        |
| chainsync.evm_blocktick_reset | Number of range fetches before trying to reset after reductions | 1        |
| chainsync.svm_rpc_permits     | Number of rpc fetches that can run concurrently in a task       | 1        |

## License

```LICENSE
MIT License

Copyright (c) Kamil Jakubus and contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
