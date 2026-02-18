CREATE TABLE IF NOT EXISTS erc20_transfers (
  contract text NOT NULL,
  from_address text NOT NULL,
  to_address text NOT NULL,
  amount_raw numeric(78,0) NOT NULL,
  tx_hash text NOT NULL,
  log_index bigint NOT NULL,
  block_number bigint NOT NULL,
  ingest_unix bigint,
  PRIMARY KEY (contract, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS erc20_transfers_contract_block_idx
  ON erc20_transfers (contract, block_number DESC);

CREATE INDEX IF NOT EXISTS erc20_transfers_from_idx
  ON erc20_transfers (from_address);

CREATE INDEX IF NOT EXISTS erc20_transfers_to_idx
  ON erc20_transfers (to_address);
