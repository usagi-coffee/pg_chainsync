CREATE FUNCTION custom_block_handler(block chainsync.Block, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Got block % from job %', block, job;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION transfer_handler(log chainsync.Log, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Got transfer % from job %', log, job;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cron_handler(log chainsync.Log, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Got transfer % from cron %', log, job;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION success_handler(job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Task succesfully executed %', job;
END;
$$ LANGUAGE plpgsql;

SELECT chainsync.register(
  'simple-blocks',
  '{
    "version": 1,
    "chain": 31337,
    "kind": "blocks",
    "provider_url": "ws://pg-chainsync-foundry:8545",
    "block_handler": "custom_block_handler"
  }'::JSONB);

SELECT chainsync.register(
  'erc20-transfer',
  '{
    "version": 1,
    "chain": 31337,
    "kind": "events",
    "provider_url": "ws://pg-chainsync-foundry:8545",
    "event_handler": "transfer_handler",
    "address": "0x5FbDB2315678afecb367f032d93F642f64180aa3",
    "event": "Transfer(address,address,uint256)"
  }'::JSONB
);

SELECT chainsync.register(
  'erc20-task',
  '{
    "version": 1,
    "chain": 31337,
    "kind": "events",
    "provider_url": "ws://pg-chainsync-foundry:8545",
    "event_handler": "transfer_handler",
    "address": "0x5FbDB2315678afecb367f032d93F642f64180aa3",
    "success_handler": "success_handler",
    "event": "Transfer(address,address,uint256)",
    "from_block": 0,
    "cron": "0 * * * * *"
  }'::JSONB
);
