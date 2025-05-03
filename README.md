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
CREATE FUNCTION custom_block_handler(block chainsync.Block, job JSONB) RETURNS blocks
AS $$
INSERT INTO blocks (number, hash) -- Inserting into your custom table
VALUES (block.number, block.hash)
RETURNING *
$$
LANGUAGE SQL;

-- Register a new job that will watch new blocks
SELECT chainsync.register(
  'simple-blocks',
  '{
    "evm": true,
    "ws": "wss://provider-url",
    "block_handler": "custom_block_handler"
  }'::JSONB);

-- Optional: Restart worker (or entire database)
SELECT chainsync.restart();
```

For the optimal performance your handler function should meet the conditions to be [inlined](https://wiki.postgresql.org/wiki/Inlining_of_SQL_functions).

Here is the complete log output, for the testing the number of fetched blocks has been limited to display the full lifecycle.

![example_output](./extra/usage1.png)

The usage examples were run on PotsgreSQL 15.

### Watching new events

```sql

-- This is your custom handler that inserts events to your table
CREATE FUNCTION custom_log_handler(log chainsync.Log, job JSONB) RETURNS logs
AS $$
INSERT INTO logs (address, data) -- Inserting into your custom table
VALUES (log.address, log.data)
RETURNING *
$$
LANGUAGE SQL;

SELECT chainsync.register(
  'custom-events',
  '{
    "evm": true,
    "ws": "ws://provider-url",
    "log_handler": "custom_log_handler",
    "address": "0x....",
    "event": "Transfer(address,address,uint256)"
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
    "evm": true,
    "ws": "ws://provider-url",
    "log_handler": "custom_log_handler",
    "address": "0x....",
    "event": "Transfer(address,address,uint256)",
    "oneshot": true,
    "from_block": 12345,
    "blocktick": 10000
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
    "evm": true,
    "ws": "wss://provider-url",
    "log_handler": "transfer_handler",
    "address": "0x....",
    "event": "Transfer(address,address,uint256)",
    "cron": "0 * * * * *",
    "from_block": 0
  }'::JSONB
);
```

#### Preloaded tasks

Some tasks need to be run when the database starts, for that you can use `preload_events_task`, the created task will run when the extension or the database re/starts.

```sql
SELECT chainsync.register(
  'transfers-on-restart',
  '{
    "evm": true,
    "ws": "wss://provider-url",
    "log_handler": "transfer_handler",
    "address": "0x....",
    "event": "Transfer(address,address,uint256)",
    "preload": true,
    "from_block": 0
  }'::JSONB
);
```

#### Handle blocks before events

`await_block` is a feature that allows you to fetch and handle event's block before handling the event. This is helpful when you want to e.g join block inside your event handler, this ensures there is always block available for your specific event when you call your event handler.

You can optionally skip block fetching and handling if you specify `block_check_handler` property which is the name of the function that takes `(block BIGINT, job JSONB)` and returns any value - if it returns any value then it will skip handling this block.

```sql
-- Look for block in your schemas and return e.g block number
CREATE FUNCTION find_block(block BIGINT, job JSONB) RETURNS BIGINT
AS $$
SELECT block_column FROM your_blocks
WHERE chain_column = job->>'chain' AND block_column = block
LIMIT 1
$$ LANGUAGE SQL;

SELECT chainsync.register(
  'transfers-every-minute',
  '{
    "evm": true,
    "ws": "wss://provider-url",
    "log_handler": "transfer_handler",
    "address": "0x....",
    "event": "Transfer(address,address,uint256)",
    "await_block": true,
    "block_handler": "insert_block",
    "block_check_handler": "find_block",
    "chain": 31337
  }'::JSONB
);

```

## Installation

> **_IMPORTANT_**: currently the database that the worker uses is hard-coded to `postgres` if you are using different database please modify the `DATABASE` constant inside `src/sync.rs` before building.

```bash
# Install pgrx
cargo install --locked cargo-pgrx

# Build the extension
cargo build --release

# Packaging process should create pg_chainsync-pg15 under target/release
cargo pgrx package

# NOTICE: your built extension and database paths may be different due to how pg_config works on the machine that builds the extension
cp target/release/pg_chainsync-.../.../pg_chainsync.so /usr/lib/postgresql/
cp target/release/pg_chainsync-.../.../pg_chainsync--....sql /usr/share/postgresql/extension/
cp target/release/pg_chainsync-.../.../pg_chainsync.control /usr/share/postgresql/extension/
```

This should be enough to be able to use `CREATE EXTENSION pg_chainsync` but we also need to preload our library because this extension uses background worker so it needs to be run along with the database.

To preload the library you need to modify `postgresql.conf` and alter `shared_preload_libraries` like that:

```
shared_preload_libraries = 'pg_chainsync.so' # (change requires restart)
```

After altering the config restart your database and you can check postgres logs to check if it worked!

> Please refer to pgrx documentation for full details on how to install background worker extension if it does not work for you

## Demo

You can check out how the extension work in action by running the development docker-compose file with `docker compose` or `podman compose`, you can find the example in `dev/dev.sql` file.

First build the extension with `cargo pgrx package` then run the docker compose command, it will run the database, run the extension and listen for some events that get sent by erc20 container.

Volumes to adjust in `docker-compose.yml` if compiled paths are different, your `pg_config` should point to your Postgres 17, keep in mind these paths will vary depending on your `pg_config`

```
- ./target/release/pg_chainsync-pg17/usr/lib64/pgsql/pg_chainsync.so:/usr/lib/postgresql/17/lib/pg_chainsync.so:z
- ./target/release/pg_chainsync-pg17/usr/share/pgsql/extension/pg_chainsync.control:/usr/share/postgresql/16/extension/pg_chainsync.control:z
- ./target/release/pg_chainsync-pg17/usr/share/pgsql/extension/pg_chainsync--0.0.0.sql:/usr/share/postgresql/16/extension/pg_chainsync--0.0.0.sql:z
```

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
