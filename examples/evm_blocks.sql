CREATE TABLE blocks (
  bl_number BIGINT PRIMARY KEY,
  bl_timestamp TIMESTAMPTZ NOT NULL,
);

CREATE FUNCTION simple_block_handler(block chainsync.EvmBlock, job JSONB) RETURNS VOID
AS $$
  INSERT INTO blocks (bl_number, bl_timestamp)
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
