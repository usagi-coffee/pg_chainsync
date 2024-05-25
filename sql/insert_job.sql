--- Insert job
INSERT INTO chainsync.jobs (
	kind,
	chain_id,
	provider_url,
	callback,
	oneshot,
  preload,
  cron,
	options
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id
