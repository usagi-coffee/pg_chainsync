create table if not exists erc20_transfers (
  contract text not null,
  from_address text not null,
  to_address text not null,
  amount_raw numeric(78,0) not null,
  tx_hash text not null,
  log_index bigint not null,
  block_number bigint not null,
  ingest_unix bigint,
  primary key (contract, tx_hash, log_index)
);

