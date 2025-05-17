CREATE TABLE logs (
  txhash TEXT NOT NULL,
  index SMALLINT NOT NULL,
  address TEXT NOT NULL,
  topics TEXT[],
  data TEXT,
  PRIMARY KEY (txhash, index)
);

CREATE FUNCTION simple_log_handler(log chainsync.EvmLog, job JSONB) RETURNS VOID
AS $$
  INSERT INTO events (ev_txhash, ev_index, ev_address, ev_topics, ev_data)
  VALUES (log.transaction_hash, log.log_index, log.address, log.topics, log.data)
  ON CONFLICT DO NOTHING
$$ LANGUAGE SQL;

-- Listen for logs
SELECT chainsync.register(
  'simple-logs',
  '{
    "evm": true,
    "ws": "ws://pg-chainsync-foundry:8545",

    "event": "Transfer(address,address,uint256)",
    "address": "5FbDB2315678afecb367f032d93F642f64180aa3",

    "log_handler": "simple_log_handler"
  }'::JSONB
);
