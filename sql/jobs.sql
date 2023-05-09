--
CREATE TABLE chainsync.jobs (
	job_type TEXT NOT NULL, -- Blocks, Events
	chain_id BIGINT NOT NULL,
	callback TEXT NOT NULL,
	provider_url TEXT NOT NULL,
	options JSONB
);