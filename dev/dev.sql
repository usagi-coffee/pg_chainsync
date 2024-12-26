CREATE EXTENSION IF NOT EXISTS pg_chainsync;

CREATE FUNCTION custom_block_handler(block chainsync.Block, job_id bigint) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Got block % from job %', block.number, job_id;
END;
$$
LANGUAGE plpgsql;

SELECT chainsync.add_blocks_job(31337, 'ws://pg-chainsync-foundry:8545', 'custom_block_handler');

CREATE FUNCTION transfer_handler(log chainsync.Log, job_id bigint) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Got transfer % from job %', log, job_id;
END;
$$
LANGUAGE plpgsql;

CREATE FUNCTION cron_handler(log chainsync.Log, job_id bigint) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Got transfer % from cron %', log, job_id;
END;
$$
LANGUAGE plpgsql;

SELECT chainsync.add_events_job(
  31337,
  'ws://pg-chainsync-foundry:8545',
  'transfer_handler',
  '{
    "address": "0x5FbDB2315678afecb367f032d93F642f64180aa3",
    "event": "Transfer(address,address,uint256)"
   }'::JSONB
);

SELECT chainsync.add_events_cron(
  31337,
  'ws://pg-chainsync-foundry:8545',
  '0 * * * * *',
  'cron_handler',
  '{
    "address": "0x5FbDB2315678afecb367f032d93F642f64180aa3",
    "event": "Transfer(address,address,uint256)",
    "from_block": 0
   }'::JSONB
);
