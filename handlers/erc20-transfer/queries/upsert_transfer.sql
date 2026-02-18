INSERT INTO erc20_transfers (
  contract,
  from_address,
  to_address,
  amount_raw,
  tx_hash,
  log_index,
  block_number,
  ingest_unix
)
VALUES (
  ($1->>'contract')::text,
  ($1->>'from')::text,
  ($1->>'to')::text,
  ($1->>'amount_raw')::numeric,
  ($1->>'tx_hash')::text,
  ($1->>'log_index')::bigint,
  ($1->>'block_number')::bigint,
  ($1->>'ingest_unix')::bigint
)
ON CONFLICT (contract, tx_hash, log_index) DO UPDATE
SET
  from_address = EXCLUDED.from_address,
  to_address = EXCLUDED.to_address,
  amount_raw = EXCLUDED.amount_raw,
  block_number = EXCLUDED.block_number,
  ingest_unix = EXCLUDED.ingest_unix;
