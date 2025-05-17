CREATE TABLE blocks (
  bl_slot BIGINT NOT NULL,
  bl_number BIGINT PRIMARY KEY,
  bl_timestamp TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION svm_block_handler(block chainsync.SvmBlock, job JSONB) RETURNS VOID
AS $$
  INSERT INTO blocks (bl_slot, bl_number, bl_timestamp)
  VALUES (block.parent_slot, block.block_height, TO_TIMESTAMP(block.block_time) AT TIME ZONE 'UTC');
$$ LANGUAGE SQL;

-- Listen for blocks
-- Provide your own ws key...
SELECT chainsync.register(
  'svm-simple-blocks',
  '{
    "svm": true,
    "ws": "...",
    "block_handler": "svm_block_handler"
  }'::JSONB
);
