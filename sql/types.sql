CREATE SCHEMA IF NOT EXISTS chainsync;

--
CREATE TABLE chainsync.jobs (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	options JSONB NOT NULL,
	status TEXT DEFAULT 'STOPPED'
);

CREATE TYPE chainsync.Block AS (
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
  block_number NUMERIC,
  block_hash TEXT,
  transaction_hash TEXT,
  transaction_index BIGINT,
	log_index BIGINT,
	address TEXT,
	topics TEXT[],
	data TEXT
);
