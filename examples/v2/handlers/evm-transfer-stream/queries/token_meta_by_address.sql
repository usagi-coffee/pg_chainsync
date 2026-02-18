SELECT decimals, symbol
FROM token_metadata
WHERE chain_id = $1 AND contract_address = $2
LIMIT 1;
