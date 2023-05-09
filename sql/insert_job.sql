--- Insert job
INSERT INTO chainsync.jobs (job_type, chain_id, provider_url, callback, options)
VALUES ($1, $2, $3, $4, $5)