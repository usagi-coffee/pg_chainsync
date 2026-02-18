INSERT INTO ohlc_1m (
  pair_id,
  bucket_start,
  open,
  high,
  low,
  close,
  volume_base,
  volume_quote,
  trades
) VALUES (
  ($1->>'pair_id')::text,
  to_timestamp(($1->>'bucket_start_unix')::bigint),
  ($1->>'open')::numeric,
  ($1->>'high')::numeric,
  ($1->>'low')::numeric,
  ($1->>'close')::numeric,
  ($1->>'volume_base')::numeric,
  ($1->>'volume_quote')::numeric,
  ($1->>'trades')::bigint
)
ON CONFLICT (pair_id, bucket_start) DO UPDATE SET
  open = EXCLUDED.open,
  high = EXCLUDED.high,
  low = EXCLUDED.low,
  close = EXCLUDED.close,
  volume_base = EXCLUDED.volume_base,
  volume_quote = EXCLUDED.volume_quote,
  trades = EXCLUDED.trades;
