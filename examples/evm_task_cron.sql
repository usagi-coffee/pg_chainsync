-- Run before the task runs, you can modify the job options used to start the task
CREATE FUNCTION cron_setup_handler(job_id INTEGER, job JSONB) RETURNS JSONB
AS $$
  UPDATE chainsync.jobs SET options['ws'] = TO_JSONB('ws://pg-chainsync-foundry:8545'::TEXT) WHERE id = job_id
  RETURNING options
$$ LANGUAGE SQL;

CREATE FUNCTION cron_log_handler(log chainsync.EvmLog, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG '[%] %', (job->>'count')::INTEGER, log;
END;
$$ LANGUAGE plpgsql;

-- Increase the count of the run
CREATE FUNCTION cron_success_handler(job_id INTEGER, job JSONB) RETURNS VOID
AS $$
  UPDATE chainsync.jobs SET options['count'] = TO_JSONB((options->>'count')::INTEGER + 1) WHERE id = job_id
$$ LANGUAGE SQL;

-- Let's fetch all logs every 1 minute
-- There is no ws key in the options, we are setting it up in the setup handler!
SELECT chainsync.register(
  'frequent-logs',
  '{
    "evm": true,

    "oneshot": true,
    "cron": "0 * * * * *",

    "from_block": 0,

    "event": "Transfer(address,address,uint256)",
    "address": "5FbDB2315678afecb367f032d93F642f64180aa3",

    "setup_handler": "cron_setup_handler",
    "log_handler": "cron_log_handler",
    "success_handler": "cron_success_handler",

    "count": 0
  }'::JSONB
);
