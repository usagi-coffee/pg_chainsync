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
CREATE FUNCTION custom_block_handler(block chainsync.Block) RETURNS blocks
AS $$
INSERT INTO blocks (number, hash) -- Inserting into your custom table
VALUES (block.number, block.hash)
RETURNING *
$$
LANGUAGE SQL;

-- The arguments are chain id, websocket url and name of the handler function
SELECT chainsync.add_blocks_job(10, 'wss://provider-url', 'custom_block_handler');

-- Restart worker (or database) to start the job
SELECT chainsync.restart();
```

For the optimal performance your handler function should meet the conditions to be [inlined](https://wiki.postgresql.org/wiki/Inlining_of_SQL_functions).

Here is the complete log output, for the testing the number of fetched blocks has been limited to display the full lifecycle.

![example_output](./extra/usage1.png)

The usage examples were run on PotsgreSQL 15.

### Watching new events

```sql

-- This is your custom handler that inserts events to your table
CREATE FUNCTION custom_event_handler(log chainsync.Log) RETURNS events
AS $$
INSERT INTO events (address, data) -- Inserting into your custom table
VALUES (log.address, log.data)
RETURNING *
$$
LANGUAGE SQL;

-- The arguments are chain id, websocket url, name of the handler function and options
SELECT chainsync.add_events_job(
	10,
	'wss://provider-url',
	'custom_event_handler',
	-- Watch every transfer event for specific contract at address
	'{ "address": "0x....", "event": "Transfer(address,address,uint256)" }'
);

-- Restart worker (or database) to start the job
SELECT chainsync.restart();
```

#### Handle blocks before events

> Experimental

`await_block` is a feature that allows you to fetch and handle event's block before handling the event. This is good when you want to e.g join block inside your event handler, this ensures there is always block available for your specific event when you call your event handler.

You can optionally skip block fetching and handling if you specify `check_block` property which is the name of the function that takes `(chain bigint, block bigint)` and returns any value - if it does then it will skip handling this block.


```sql
SELECT chainsync.add_events_job(
	10,
	'wss://provider-url',
	'custom_event_handler',
	-- Watch every transfer event for specific contract at address
	'{ 
	    "address": "0x....",
	    "event": "Transfer(address,address,uint256)",

	    "await_block": {
	        "check_block": "select_one_block",
	        "block_handler": "insert_block"
	    }
	}'
);

```

## Installation

> ***IMPORTANT***: currently the database that the worker uses is hard-coded to `postgres` if you are using different database please modify the `DATABASE` constant inside `src/sync.rs` before building.

```bash
# Build the extension
cargo build --release

# Packaging process should create pg_chainsync-pg15 under target/release
cargo pgx package

# Copy files to your postgres installation (paths may be different on your system)
cd target/release
cp pg_chainsync-pg15/.../pg_chainsync.so /usr/lib/postgresql/
# Replace V.V.V with version
cp pg_chainsync-pg15/.../pg_chainsync--V.V.V.sql /usr/share/postgresql/extension/
cp pg_chainsync-pg15/.../pg_chainsync.control /usr/share/postgresql/extension/  
```

This should be enough to be able to use `CREATE EXTENSION pg_chainsync` but we also need to preload our library because this extension uses background worker so it needs to be run along with the database.

To preload the library you need to modify `postgresql.conf` and alter `shared_preload_libraries` like that:

```
shared_preload_libraries = 'pg_chainsync.so' # (change requires restart)
```

After altering the config restart your database and you can check postgres logs to check if it worked!

> Please refer to pgrx documentation for full details on how to install background worker extension if it does not work for you

## License

```LICENSE
MIT License

Copyright (c) 2023 Kamil Jakubus and contributors

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
