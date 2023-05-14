--
CREATE TABLE chainsync.jobs (
	id SERIAL NOT NULL,
	kind TEXT NOT NULL, -- Blocks, Events
	chain_id BIGINT NOT NULL,
	callback TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'STOPPED',
	provider_url TEXT NOT NULL,
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
	number BIGINT,
	gas_used BIGINT,
	gas_limit BIGINT,
	base_fee_per_gas BIGINT,
	extra_data TEXT,
	timestamp TIMESTAMPTZ,
	difficulty BIGINT,
	total_difficulty BIGINT,
	size BIGINT
);

CREATE TYPE chainsync.Log AS (
	removed BOOLEAN,
	log_index BIGINT,
	transaction_hash TEXT,
	transaction_index BIGINT,
	transaction_log_index BIGINT,
	block_hash TEXT,
	block_number BIGINT,
	address TEXT,
	data TEXT,
	topics TEXT[]
);