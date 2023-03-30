# pg_chainsync - Watch blockchain inside PostgreSQL

> Proof of Concept - expect bugs and breaking changes.

pg_chainsync aims to allow you to easily watch blockchain blocks, events and more directly inside your PostgreSQL instance. The extension does not enforce any custom schema for your table and let's you use custom handlers that you adjust for your specific use-case (you don't even need to insert if you want!).

The extension is written using [pgx](https://github.com/tcdi/pgx) in Rust.

## Usage

Let's watch blocks and insert them to your custom table
> For the usage let's assume you've got blocks table with number and hash column, the extension does not enforce how your schema looks, it's up to you!

```sql
-- Let's use the extension
CREATE EXTENSION pg_chainsync;

-- This is your custom handler and your custom table
-- Block type is pg_chainsync type
CREATE FUNCTION custom_block_handler(block Block) RETURNS blocks
AS $$
INSERT INTO blocks (number, hash)
VALUES (block.number, block.hash)
RETURNING *
$$
LANGUAGE SQL;

-- The arguments are chain id, websocket url and name of the handler function
SELECT add_blocks_job(10, "wss://your-provider-url", "custom_block_handler");

-- Restart your database to run the job or call restart on-demand
SELECT chainsync.restart();
```

Here is the log output after the job has started, for the testing the number of fetched blocks has been limited - it obviously can run indefinitely.
![example_output](./extra/usage1.png)

The usage example was run on Potsgresql 15.

## Installation

> The extension is currently in Proof of Concept stage and does not provide instructions how to install it. Refer to the pgx documentation to see how to run the extension that uses worker if you are interested in trying it out.


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
