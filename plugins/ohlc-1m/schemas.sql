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

CREATE INDEX IF NOT EXISTS ohlc_1m_bucket_idx
  ON ohlc_1m (bucket_start_unix);
