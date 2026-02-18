\i examples/v2/ohlc_schema.sql

CREATE EXTENSION IF NOT EXISTS pg_chainsync;

SELECT chainsync.reload();
SELECT chainsync.restart();
