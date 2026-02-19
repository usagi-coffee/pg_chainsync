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


CREATE TABLE IF NOT EXISTS ohlc_1m (
  pair_id text NOT NULL,
  bucket_start_unix bigint NOT NULL,
  open double precision NOT NULL,
  high double precision NOT NULL,
  low double precision NOT NULL,
  close double precision NOT NULL,
  volume_base double precision NOT NULL DEFAULT 0,
  volume_quote double precision NOT NULL DEFAULT 0,
  trades bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (pair_id, bucket_start_unix)
);
