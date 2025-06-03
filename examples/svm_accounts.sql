CREATE TABLE accounts (
  address TEXT PRIMARY KEY,
  balance NUMERIC DEFAULT 0,
  ui_balance NUMERIC DEFAULT 0
);

CREATE FUNCTION svm_account_handler(acc chainsync.SvmAccount, job JSONB) RETURNS VOID
AS $$
DECLARE
  amount NUMERIC;
BEGIN
  SELECT
    get_byte(acc.data, 0) +
    get_byte(acc.data, 1) * 256 +
    get_byte(acc.data, 2) * 256^2 +
    get_byte(acc.data, 3) * 256^3 +
    get_byte(acc.data, 4) * 256^4 +
    get_byte(acc.data, 5) * 256^5 +
    get_byte(acc.data, 6) * 256^6 +
    get_byte(acc.data, 7) * 256^7
  INTO amount;

  INSERT INTO accounts (address, balance, ui_balance)
  VALUES (acc.pubkey, amount, amount / POW(10, (job->>'decimals')::SMALLINT))
  ON CONFLICT (address) DO UPDATE SET data = EXCLUDED.data, balance = EXCLUDED.balance, ui_balance = EXCLUDED.ui_balance;
END;
$$ LANGUAGE plpgsql;

-- Get all accounts for the given token
SELECT chainsync.register(
  'svm-accounts',
  '{
    "rpc": "...",
    "oneshot": true,
    "preload": true,

    "svm": {
      "account_handler": "svm_account_handler",
      "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "accounts_data_slice": { "offset": 64, "length": 8 },
      "accounts_filters": [
        { "dataSize": 165 },
        { "memcmp": { "offset": 0, "bytes": "..." } }
      ]
    },

    "decimals": 6
  }'::JSONB
);
