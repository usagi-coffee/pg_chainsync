--- Insert job
INSERT INTO chainsync.jobs (job_type, chain_id, provider_url, callback)
VALUES ($1, $2, $3, $4)