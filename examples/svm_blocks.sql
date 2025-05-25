CREATE TABLE blocks (
  slot BIGINT NOT NULL,
  number BIGINT PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION svm_block_handler(block chainsync.SvmBlock, job JSONB) RETURNS VOID
AS $$
  INSERT INTO blocks (slot, number, timestamp)
  VALUES (block.parent_slot, block.block_height, TO_TIMESTAMP(block.block_time) AT TIME ZONE 'UTC');
$$ LANGUAGE SQL;

-- Listen for blocks
-- Provide your own ws key...
SELECT chainsync.register(
  'svm-simple-blocks',
  '{
    "ws": "...",
    "svm": {
      "block_handler": "svm_block_handler"
    }
  }'::JSONB
);
