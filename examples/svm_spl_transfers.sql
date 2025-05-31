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
  discriminator SMALLINT;
  source TEXT;
  destination TEXT;
  amount BIGINT;
BEGIN
  discriminator := get_byte(inst.data, 0);

  IF discriminator = 3 THEN -- Transfer
    -- NOTICE: Not always there will be an account mint mentioned in the transaction!
    -- To properly handle Transfer you need an additional source of account -> mint mapping, like accounts table (address, owner, mint)
    -- You can use account_mint_lookup feature to inject mint into accounts_mints from your accounts table!
    IF inst.accounts_mints[1] IS DISTINCT FROM (job->>'token')::TEXT THEN RETURN; END IF; -- Mint NOT always mentioned!

    source := inst.accounts[1]; -- or accounts_owners[1]
    destination := inst.accounts[2]; -- or accounts_owners[2]
    RAISE LOG 'Transfer % -> % amount: %', source, destination, amount;
  ELSIF discriminator = 12 THEN -- TransferChecked
    IF inst.accounts[2] IS DISTINCT FROM (job->>'token')::TEXT THEN RETURN; END IF; -- Mint provided in accounts
    source := inst.accounts[1]; -- or accounts_owners[1]
    destination := inst.accounts[3]; -- or accounts_owners[3]
    RAISE LOG 'Transfer % -> % amount: %', source, destination, amount;
  ELSIF discriminator = 7 THEN -- MintTo
    IF inst.accounts[1] IS DISTINCT FROM (job->>'token')::TEXT THEN RETURN; END IF; -- Mint provided in accounts
    source := '';
    destination := inst.accounts[2]; -- or accounts_owners[1]
    RAISE LOG 'Mint % amount: %', destination, amount;
  ELSIF discriminator = 8 THEN -- Burn
    IF inst.accounts[2] IS DISTINCT FROM (job->>'token')::TEXT THEN RETURN; END IF; -- Mint provided in accounts
    source := inst.accounts[1]; -- or accounts_owners[1]
    destination := '';
    RAISE LOG 'Burn % amount: %', source, amount;
  ELSIF discriminator = 14 THEN -- MintToChecked
    IF inst.accounts[1] IS DISTINCT FROM (job->>'token')::TEXT THEN RETURN; END IF; -- Mint provided in accounts
    source      := '';
    destination := inst.accounts[2];
    RAISE LOG 'MintToChecked % amount: %', destination, amount;
  ELSIF discriminator = 15 THEN -- BurnChecked
    IF inst.accounts[2] IS DISTINCT FROM (job->>'token')::TEXT THEN RETURN; END IF; -- Mint provided in accounts
    source      := inst.accounts[1];
    destination := '';
    RAISE LOG 'BurnChecked % amount: %', source, amount;
  END IF;

  -- Calculate the amount from the instruction data, this is probably slower than using custom extension function
  SELECT
    get_byte(inst.data, 1) +
    get_byte(inst.data, 2) * 256 +
    get_byte(inst.data, 3) * 256^2 +
    get_byte(inst.data, 4) * 256^3 +
    get_byte(inst.data, 5) * 256^4 +
    get_byte(inst.data, 6) * 256^5 +
    get_byte(inst.data, 7) * 256^6 +
    get_byte(inst.data, 8) * 256^7
  INTO amount;
  -- Convert the amount to the proper value using the decimals from the job
  amount := amount / POWER(10, (job->>'decimals')::SMALLINT);

  INSERT INTO transfers (source, destination, amount, timestamp)
  VALUES (source, destination, amount, TO_TIMESTAMP(inst.block_time) AT TIME ZONE 'UTC')
  ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;

SELECT chainsync.register(
  'svm-transfers',
  '{
    "ws": "<redacted>",
    "rpc": "<redacted>",
    "oneshot": true,

    "svm": {
      "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "mentions": ["<your_token>"],
      "instruction_handler": "svm_transfer_handler",
      "instruction_discriminators": [3, 7, 8, 12, 14, 15]
    },

    "address": "<your_token>",
    "decimals": 6,
  }'::JSONB
);
