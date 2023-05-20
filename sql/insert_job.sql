--- Insert job
INSERT INTO chainsync.jobs (
	kind,
	chain_id,
	provider_url,
	callback,
	oneshot,
	options
)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id