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
  INSERT INTO logs (txhash, index, address, topics, data)
  VALUES (log.transaction_hash, log.log_index, log.address, log.topics, log.data)
  ON CONFLICT DO NOTHING
$$ LANGUAGE SQL;

-- Listen for logs
SELECT chainsync.register(
  'simple-logs',
  '{
    "ws": "ws://pg-chainsync-foundry:8545",
    "evm": {
      "event": "Transfer(address,address,uint256)",
      "address": "5FbDB2315678afecb367f032d93F642f64180aa3",
      "log_handler": "simple_log_handler"
    }
  }'::JSONB
);
