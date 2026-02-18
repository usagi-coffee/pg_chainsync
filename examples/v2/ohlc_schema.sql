CREATE TABLE IF NOT EXISTS ohlc_1m (
  pair_id TEXT NOT NULL,
  bucket_start TIMESTAMPTZ NOT NULL,
  open NUMERIC NOT NULL,
  high NUMERIC NOT NULL,
  low NUMERIC NOT NULL,
  close NUMERIC NOT NULL,
  volume_base NUMERIC NOT NULL,
  volume_quote NUMERIC NOT NULL,
  trades BIGINT NOT NULL,
  PRIMARY KEY (pair_id, bucket_start)
);

CREATE TABLE IF NOT EXISTS token_pool_meta (
  pool_address TEXT PRIMARY KEY,
  base_decimals INTEGER NOT NULL,
  quote_decimals INTEGER NOT NULL
);
