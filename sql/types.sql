CREATE SCHEMA IF NOT EXISTS chainsync;

--
CREATE TABLE chainsync.jobs (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	options JSONB NOT NULL,
	status TEXT DEFAULT 'STOPPED'
);

CREATE TYPE chainsync.EvmBlock AS (
  number NUMERIC,
	hash TEXT,
	author TEXT,
	difficulty NUMERIC,
	total_difficulty NUMERIC,
	state_root TEXT,
	parent_hash TEXT,
	omners_hash TEXT,
	transactions_root TEXT,
	receipts_root TEXT,
	gas_used NUMERIC,
	gas_limit NUMERIC,
	size NUMERIC,
	timestamp NUMERIC
);

CREATE TYPE chainsync.EvmLog AS (
  block_number NUMERIC,
  block_hash TEXT,
  transaction_hash TEXT,
  transaction_index BIGINT,
	log_index BIGINT,
	address TEXT,
	topics TEXT[],
	data TEXT
);

CREATE TYPE chainsync.SvmInstruction AS (
  slot NUMERIC,
  signature TEXT,
  accounts TEXT[],
  accounts_owners TEXT[],
  accounts_mints TEXT[],
  program_id TEXT,
  index SMALLINT,
  inner_index SMALLINT,
  data BYTEA,
  block_time BIGINT
);

CREATE TYPE chainsync.SvmTransaction AS (
  slot NUMERIC,
  signature TEXT,
  accounts TEXT[],
  instructions chainsync.SvmInstruction[],
  logs TEXT[],
  signatures TEXT[],
  block_time BIGINT
);

CREATE TYPE chainsync.SvmBlock AS (
  parent_slot NUMERIC,
  block_height NUMERIC,
  block_hash TEXT,
  previous_block_hash TEXT,
  transactions chainsync.SvmTransaction[],
  signatures TEXT[],
  block_time BIGINT
);

CREATE TYPE chainsync.SvmAccount AS (
  pubkey TEXT,
  data BYTEA,
  executable BOOLEAN,
  lamports NUMERIC,
  owner TEXT,
  rent_epoch NUMERIC
);

CREATE TYPE chainsync.SvmLog AS (
  slot_number NUMERIC,
  signature TEXT,
  logs TEXT[]
);
