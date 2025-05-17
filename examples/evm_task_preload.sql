CREATE FUNCTION preload_log_handler(log chainsync.EvmLog, job JSONB) RETURNS VOID
AS $$
  RAISE LOG 'Got log % at %', log, CURRENT_TIMESTAMP;
$$ LANGUAGE plpgsql;

-- Let's fetch all logs at the database start
SELECT chainsync.register(
  'preload-logs',
  '{
    "evm": true,
    "ws": "ws://pg-chainsync-foundry:8545",

    "oneshot": true,
    "preload": true,

    "from_block": 0,

    "event": "Transfer(address,address,uint256)",
    "address": "5FbDB2315678afecb367f032d93F642f64180aa3",

    "log_handler": "preload_log_handler"
  }'::JSONB
);
