SELECT
  base_decimals,
  quote_decimals
FROM token_pool_meta
WHERE pool_address = '${POOL_ADDRESS}'
LIMIT 1
