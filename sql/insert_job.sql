--- Insert job
INSERT INTO chainsync.jobs (kind, chain_id, provider_url, callback, options)
VALUES ($1, $2, $3, $4, $5)