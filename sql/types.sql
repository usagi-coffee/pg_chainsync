CREATE SCHEMA IF NOT EXISTS chainsync;

--
CREATE TABLE chainsync.jobs (
	id SERIAL NOT NULL,
	kind TEXT NOT NULL, -- Blocks, Events
	chain_id BIGINT NOT NULL,
	callback TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'STOPPED',
	provider_url TEXT NOT NULL,
	oneshot BOOLEAN NOT NULL,
  preload BOOLEAN NOT NULL DEFAULT FALSE,
  cron TEXT,
	options JSONB
);

CREATE TYPE chainsync.Block AS (
  chain BIGINT,
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

CREATE TYPE chainsync.Log AS (
  chain BIGINT,
  block_number NUMERIC,
  block_hash TEXT,
  transaction_hash TEXT,
  transaction_index BIGINT,
	log_index BIGINT,
	address TEXT,
	topics TEXT[],
	data TEXT
);
