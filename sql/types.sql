--
CREATE TABLE chainsync.jobs (
	id SERIAL NOT NULL,
	kind TEXT NOT NULL, -- Blocks, Events
	chain_id BIGINT NOT NULL,
	callback TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'STOPPED',
	provider_url TEXT NOT NULL,
	oneshot BOOLEAN NOT NULL,
	options JSONB
);

CREATE TYPE chainsync.Block AS (
	hash TEXT,
	author TEXT,
	state_root TEXT,
	parent_hash TEXT,
	uncles_hash TEXT,
	transactions_root TEXT,
	receipts_root TEXT,
	number NUMERIC,
	gas_used NUMERIC,
	gas_limit NUMERIC,
	base_fee_per_gas NUMERIC,
	extra_data TEXT,
	timestamp NUMERIC,
	difficulty NUMERIC,
	total_difficulty NUMERIC,
	size NUMERIC,
	chain BIGINT
);

CREATE TYPE chainsync.Log AS (
	removed BOOLEAN,
	log_index BIGINT,
	transaction_hash TEXT,
	transaction_index BIGINT,
	transaction_log_index BIGINT,
	block_hash TEXT,
	block_number NUMERIC,
	address TEXT,
	data TEXT,
	topics TEXT[],
	chain BIGINT
);