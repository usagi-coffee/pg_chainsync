CREATE TABLE transfers (
  source TEXT NOT NULL,
  destination TEXT NOT NULL,
  amount BIGINT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (source, destination, timestamp)
);

CREATE FUNCTION svm_transfer_handler(inst chainsync.SvmInstruction, job JSONB) RETURNS VOID
AS $$
DECLARE
  bytes BYTEA;
  discriminator SMALLINT;
  source TEXT;
  destination TEXT;
  amount BIGINT;
BEGIN
  -- Filter out transfers that are not from the specified token
  IF inst.accounts_mints[1] <> (job->'mentions'->>0)::TEXT THEN
    RETURN;
  END IF;

  bytes := base58.decode(inst.data);

  SELECT
    get_byte(bytes, 1) +
    get_byte(bytes, 2) * 256 +
    get_byte(bytes, 3) * 256^2 +
    get_byte(bytes, 4) * 256^3 +
    get_byte(bytes, 5) * 256^4 +
    get_byte(bytes, 6) * 256^5 +
    get_byte(bytes, 7) * 256^6 +
    get_byte(bytes, 8) * 256^7
  INTO amount;
  amount := amount / POWER(10, (job->>'decimals')::SMALLINT);

  discriminator := get_byte(bytes, 0);
  IF discriminator = 3 THEN -- Transfer
    source := inst.accounts[1]; -- or accounts_owners[1]
    destination := inst.accounts[2]; -- or accounts_owners[2]
    RAISE LOG 'Transfer % -> % amount: %', source, destination, amount;
  ELSIF discriminator = 12 THEN -- TransferChecked
    source := inst.accounts[1]; -- or accounts_owners[1]
    destination := inst.accounts[3]; -- or accounts_owners[3]
    RAISE LOG 'Transfer % -> % amount: %', source, destination, amount;
  ELSIF discriminator = 7 THEN -- MintTo
    source := '';
    destination := inst.accounts[2]; -- or accounts_owners[1]
    RAISE LOG 'Mint % amount: %', destination, amount;
  ELSIF discriminator = 8 THEN -- Burn
    source := inst.accounts[1]; -- or accounts_owners[1]
    destination := '';
    RAISE LOG 'Burn % amount: %', source, amount;
  END IF;


  INSERT INTO transfers (source, destination, amount, timestamp)
  VALUES (source, destination, amount, TO_TIMESTAMP(inst.block_time) AT TIME ZONE 'UTC')
  ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;

SELECT chainsync.register(
  'svm-transfers',
  '{
    "svm": true,
    "ws": "<redacted>",
    "rpc": "<redacted>",

    "oneshot": true,

    "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "mentions": ["<your_token>"],
    "instruction_handler": "svm_transfer_handler",
    "instruction_discriminators": [3, 12, 7, 8],

    "decimals": 6
  }'::JSONB
);
