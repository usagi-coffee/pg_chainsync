-- This is a demo script that collects logs from the local foundry testnet
-- It collects logs and blocks in real-time (unverified) and verifies them later with a cron task

-- For convenience let's use case insensitive collation for the txhash and address (evm is case insensitive)
CREATE COLLATION IF NOT EXISTS case_insensitive (
  provider = icu,
  locale = 'und-u-ks-level2',
  deterministic = false
);

-- Table to store blocks, for most common cases you only need timestamp
CREATE TABLE blocks (
  bl_number BIGINT PRIMARY KEY,
  bl_timestamp TIMESTAMPTZ NOT NULL
);

-- Function to find blocks
CREATE OR REPLACE FUNCTION find_block(block BIGINT, job JSONB) RETURNS BIGINT
AS $$ SELECT bl_number FROM blocks WHERE bl_number = block LIMIT 1 $$ LANGUAGE SQL STABLE;

-- Function to insert blocks
CREATE FUNCTION block_handler(block chainsync.EvmBlock, job JSONB) RETURNS VOID
AS $$
  INSERT INTO blocks (bl_number, bl_timestamp)
  VALUES (block.number, TO_TIMESTAMP(block.timestamp) AT TIME ZONE 'UTC')
  ON CONFLICT DO NOTHING
$$ LANGUAGE SQL;

-- Table to store logs
CREATE TABLE logs (
  lo_txhash TEXT NOT NULL COLLATE "case_insensitive",
  lo_index SMALLINT NOT NULL,
  lo_block BIGINT NOT NULL,
  lo_timestamp TIMESTAMPTZ NOT NULL,

  lo_address TEXT NOT NULL COLLATE "case_insensitive",
  -- lo_event TEXT, -- we dont need it in this demo but you should also store it
  lo_topics TEXT[],
  lo_data TEXT,

  -- verified | unverified
  lo_source TEXT DEFAULT 'unverified',
  PRIMARY KEY (lo_txhash, lo_index)
);

-- In this demo we don't decode the logs, but you can do it directly in the handler!
CREATE FUNCTION log_handler(log chainsync.EvmLog, job JSONB) RETURNS VOID
AS $$
  INSERT INTO logs (lo_txhash, lo_index, lo_block, lo_timestamp, lo_address, lo_topics, lo_data, lo_source)
  VALUES (
    log.transaction_hash,
    log.log_index,
    log.block_number,
    -- Get the timestamp from the blocks table, await block handler ensures that the block is already in the table :)
    (SELECT bl_timestamp FROM blocks WHERE bl_number = log.block_number),
    log.address,
    log.topics,
    log.data,
    (job->>'source')::TEXT
  )
  -- This is crucial to make sure that the logs get confirmed by the verify task
  ON CONFLICT (lo_txhash, lo_index) DO UPDATE SET lo_source = (job->>'source')::TEXT;
$$ LANGUAGE SQL;

-- Task that gets the logs in real-time and marks them as unverified
SELECT chainsync.register(
  'logs',
  '{
    "evm": true,
    "ws": "ws://pg-chainsync-foundry:8545",

    "log_handler": "log_handler",

    "address": "5FbDB2315678afecb367f032d93F642f64180aa3",
    "event": "Transfer(address,address,uint256)",

    "await_block": true,
    "block_handler": "block_handler",
    "block_check_handler": "find_block",

    "source": "unverified"
  }'::JSONB
);

-- Index to speed up the verify queries
CREATE INDEX logs_source ON logs (lo_address, lo_source);

-- Handler that updates the options before the task runs
CREATE FUNCTION sweep_reset(job_id INTEGER, job JSONB) RETURNS JSONB
AS $$
DECLARE
  from_block BIGINT;
  updated JSONB;
BEGIN
  -- Fetch latest verified block
  SELECT lo_block FROM logs
  WHERE lo_address = (job->>'address')::TEXT AND lo_source = 'verified'
  ORDER BY lo_block DESC LIMIT 1
  INTO from_block;

  RAISE LOG 'Setting from_block to the latest verified event at %', COALESCE(from_block, 0);

  -- Update the from_block
  UPDATE chainsync.jobs SET options['from_block'] = TO_JSONB(COALESCE(from_block, 0))
  WHERE id = job_id
  RETURNING options INTO updated;

  -- Make sure to return the updated options in setup handler!
  RETURN updated;
END;
$$ LANGUAGE plpgsql;

-- Remove logs with unverified source below the latest verified to make sure no orphaned logs are left
CREATE FUNCTION sweep_unverified(job_id INTEGER, job JSONB) RETURNS VOID
AS $$
DECLARE
  verified_block BIGINT;
  deleted_count INTEGER;
BEGIN
  -- Fetch latest verified block
  SELECT lo_block FROM logs
  WHERE lo_address = (job->>'address')::TEXT AND lo_source = 'verified'
  ORDER BY lo_block DESC LIMIT 1
  INTO verified_block;

  -- Delete logs with unverified source below the cutoff - these should be orphaned
  DELETE FROM logs
  WHERE
    lo_address = (job->>'address')::TEXT AND
    lo_source = 'unverified' AND
    lo_block <= COALESCE(verified_block, 0);

  -- Log the number of deleted logs (most likely orphaned)
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  IF deleted_count > 0 THEN
     RAISE LOG 'Orphaned logs count: %', deleted_count;
   END IF;
END;
$$ LANGUAGE plpgsql;

-- Task that runs every 1 minute to verify the logs in case of orphaned blocks
-- Here the safe limit is set up as 15 block confirmations from latest block (-15 value)
SELECT chainsync.register(
  'verify-logs',
  '{
    "evm": true,
    "ws": "ws://pg-chainsync-foundry:8545",

    "cron": "0 * * * * *",
    "from_block": 0,
    "to_block": -15,

    "log_handler": "log_handler",

    "setup_handler": "sweep_reset",
    "success_handler": "sweep_unverified",

    "address": "5FbDB2315678afecb367f032d93F642f64180aa3",
    "event": "Transfer(address,address,uint256)",

    "await_block": true,
    "block_handler": "custom_block_handler",
    "block_check_handler": "find_block",

    "source": "verified"
  }'::JSONB
);
