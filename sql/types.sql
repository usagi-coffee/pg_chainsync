--
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