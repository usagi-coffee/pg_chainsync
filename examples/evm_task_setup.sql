-- Setup
CREATE FUNCTION cron_log_handler(log chainsync.EvmLog, job JSONB) RETURNS JSONB
AS $$
  UPDATE chainsync.jobs SET options['now'] = TO_JSONB(CURRENT_TIMESTAMP)
  RETURNING options
$$ LANGUAGE SQL;

CREATE FUNCTION cron_block_handler(log chainsync.EvmBlock, job JSONB) RETURNS JSON
AS $$
  RAISE LOG '%', log;
$$ LANGUAGE plpgsql;

SELECT chainsync.register(
  'setup-every-minute',
  '{
    "evm": true,
    "ws": "ws://pg-chainsync-foundry:8545",

    "oneshot": true,
    "cron": "0 * * * * *",

    "from_block": 0,

    "block_handler": "preload_log_handler"
  }'::JSONB
);
