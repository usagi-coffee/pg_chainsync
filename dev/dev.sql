-- It's nice to have case insensitive collation for addresses and hashes
CREATE COLLATION IF NOT EXISTS case_insensitive (
  provider = icu,
  locale = 'und-u-ks-level2',
  deterministic = false
);

-- Table to store blocks, for most common cases you only need timestamp
CREATE TABLE blocks (
  bl_chain TEXT COLLATE "case_insensitive",
  bl_number BIGINT,
  bl_timestamp TIMESTAMPTZ NOT NULL,

  -- for solana
  bl_slot BIGINT DEFAULT 0,

  PRIMARY KEY (bl_chain, bl_number)
);

-- Function to find blocks
CREATE OR REPLACE FUNCTION find_block(block BIGINT, job JSONB) RETURNS BIGINT
AS $$
  SELECT bl_number FROM blocks WHERE bl_chain = job->>'chain' AND bl_number = block LIMIT 1
$$ LANGUAGE SQL STABLE;

-- Table to store log events
CREATE TABLE events (
  ev_chain TEXT COLLATE "case_insensitive",
  ev_block BIGINT NOT NULL,
  ev_contract TEXT NOT NULL COLLATE "case_insensitive",
  ev_txhash TEXT NOT NULL COLLATE "case_insensitive",
  ev_index SMALLINT NOT NULL,
  ev_timestamp TIMESTAMPTZ NOT NULL,
  ev_source TEXT DEFAULT NULL,

  PRIMARY KEY (ev_chain, ev_contract, ev_txhash, ev_index)
);

-- Helps when updating the source to verified
CREATE INDEX ev_idx_source ON events (ev_contract, ev_source, ev_block);

-- Function to insert blocks
CREATE FUNCTION custom_block_handler(block chainsync.EvmBlock, job JSONB) RETURNS VOID
AS $$
  INSERT INTO blocks (bl_chain, bl_number, bl_timestamp)
  VALUES (job->>'chain', block.number, TO_TIMESTAMP(block.timestamp) AT TIME ZONE 'UTC')
  ON CONFLICT DO NOTHING
$$ LANGUAGE SQL;

SELECT chainsync.register(
  'gather-blocks',
  '{
    "evm": true,
    "ws": "ws://pg-chainsync-foundry:8545",
    "block_handler": "custom_block_handler",
    "chain": 31337
  }'::JSONB
);

-- Function to insert event logs
CREATE FUNCTION transfer_handler(log chainsync.EvmLog, job JSONB) RETURNS VOID
AS $$
  INSERT INTO events (ev_chain, ev_block, ev_contract, ev_txhash, ev_index, ev_timestamp, ev_source)
  VALUES (
    job->>'chain',
    log.block_number,
    log.address,
    log.transaction_hash,
    log.log_index,
    (SELECT bl_timestamp FROM blocks WHERE bl_chain = job->>'chain' AND bl_number = log.block_number),
    job->>'source'
  )
  ON CONFLICT (ev_chain, ev_contract, ev_txhash, ev_index) DO UPDATE SET ev_source = job->>'source';
$$ LANGUAGE SQL;

-- Handler that changes the options before the tasks starts
CREATE FUNCTION sweep_reset(job_id INTEGER, job JSONB) RETURNS JSONB
AS $$
DECLARE
  cutoff_block BIGINT;
  updated_options JSONB;
BEGIN
  -- Fetch latest verified block
  SELECT ev_block FROM events
  WHERE ev_chain = job->>'chain' AND ev_contract = job->>'address' AND ev_source = 'verified'
  ORDER BY ev_block DESC LIMIT 1
  INTO cutoff_block;

  RAISE LOG 'Setting from_block to %', COALESCE(cutoff_block, 0);

  -- Update the from_block
  UPDATE chainsync.jobs SET options['from_block'] = TO_JSONB(COALESCE(cutoff_block, 0))
  WHERE id = job_id
  RETURNING options INTO updated_options;

  RETURN updated_options;
END;
$$ LANGUAGE plpgsql;

-- Remove events with unverified source below the cutoff to make sure no orphaned events are left
CREATE FUNCTION sweep_unverified(job_id INTEGER, job JSONB) RETURNS VOID
AS $$
  DELETE FROM events
  WHERE ev_chain = job->>'chain' AND ev_contract = job->>'address' AND ev_source = 'unverified' AND ev_block <= COALESCE((job->>'from_block')::BIGINT, 0)
$$ LANGUAGE SQL;

-- Task that gets all transfers in real-time

SELECT chainsync.register(
  'erc20-transfer',
  '{
    "evm": true,

    "ws": "ws://pg-chainsync-foundry:8545",

    "log_handler": "transfer_handler",

    "address": "5FbDB2315678afecb367f032d93F642f64180aa3",
    "event": "Transfer(address,address,uint256)",

    "await_block": true,
    "block_handler": "custom_block_handler",
    "block_check_handler": "find_block",

    "source": "unverified",
    "chain": 31337
  }'::JSONB
);

-- Task that runs every 1 minute to verify the transfers in case of orphaned blocks
-- Here the safe limit is set up as 15 block confirmations from latest block (-15 value)
SELECT chainsync.register(
  'erc20-transfer-verify',
  '{
    "evm": true,

    "ws": "ws://pg-chainsync-foundry:8545",

    "cron": "0 * * * * *",
    "from_block": 0,
    "to_block": -15,


    "log_handler": "transfer_handler",

    "setup_handler": "sweep_reset",
    "success_handler": "sweep_unverified",

    "address": "5FbDB2315678afecb367f032d93F642f64180aa3",
    "event": "Transfer(address,address,uint256)",

    "await_block": true,
    "block_handler": "custom_block_handler",
    "block_check_handler": "find_block",

    "source": "verified",
    "chain": 31337
  }'::JSONB
);

---- Solana support

-- Function to insert blocks for Solana
CREATE FUNCTION custom_svm_block_handler(block chainsync.SvmBlock, job JSONB) RETURNS VOID
AS $$
  INSERT INTO blocks (bl_chain, bl_slot, bl_number, bl_timestamp)
  VALUES (job->>'chain', block.parent_slot, block.block_height, TO_TIMESTAMP(block.block_time) AT TIME ZONE 'UTC');
$$ LANGUAGE SQL;

-- Log handler for Solana
CREATE FUNCTION custom_svm_log_handler(log chainsync.SvmLog, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Processing log %', log;
END;
$$ LANGUAGE plpgsql;

-- Transaction handler for Solana
CREATE FUNCTION custom_svm_transaction_handler(tx chainsync.SvmTransaction, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Processing transaction %', tx.signature;
END;
$$ LANGUAGE plpgsql;

-- Instruction handler for Solana
CREATE FUNCTION custom_svm_instruction_handler(inst chainsync.SvmInstruction, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Processing instruction [%] #%.%: data: %', inst.program_id, inst.index, COALESCE(inst.inner_index, 0), inst.data;
END;
$$ LANGUAGE plpgsql;

/*
SELECT chainsync.register(
  'sol-token-instrutions',
  '{
    "svm": true,
    "oneshot": true,
    "preload": true,

    "rpc": "...",
    "ws": "...",

    "transaction_handler": "custom_svm_transaction_handler",
    "instruction_handler": "custom_svm_instruction_handler",

    "mentions": ["..."],
    "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",

    "chain": "solana",
    "source": "unverified"
  }'::JSONB
);
*/
