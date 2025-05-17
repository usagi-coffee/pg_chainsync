CREATE TABLE blocks (
  number BIGINT PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NUll
);

CREATE FUNCTION simple_block_handler(block chainsync.EvmBlock, job JSONB) RETURNS VOID
AS $$
  INSERT INTO blocks (number, timestamp)
  VALUES (block.number, TO_TIMESTAMP(block.timestamp) AT TIME ZONE 'UTC')
$$ LANGUAGE SQL;

-- Listen for blocks
SELECT chainsync.register(
  'simple-blocks',
  '{
    "evm": true,
    "ws": "ws://pg-chainsync-foundry:8545",
    "block_handler": "simple_block_handler"
  }'::JSONB
);
