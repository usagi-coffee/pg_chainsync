INSERT INTO token_transfers (
  chain_id,
  tx_hash,
  log_index,
  contract_address,
  from_address,
  to_address,
  amount_raw,
  amount_decimal,
  block_number,
  block_time
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (chain_id, tx_hash, log_index) DO UPDATE SET
  contract_address = EXCLUDED.contract_address,
  from_address = EXCLUDED.from_address,
  to_address = EXCLUDED.to_address,
  amount_raw = EXCLUDED.amount_raw,
  amount_decimal = EXCLUDED.amount_decimal,
  block_number = EXCLUDED.block_number,
  block_time = EXCLUDED.block_time;
