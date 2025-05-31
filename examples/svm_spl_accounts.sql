-- Store the history of the accounts here
-- In this example we do not track closed accounts
CREATE TABLE accounts (
  address TEXT,
  owner TEXT,
  mint TEXT,
  closed BOOLEAN,
  signature TEXT NOT NULL,
  changed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (address, signature)
);

CREATE UNIQUE INDEX accounts_changed ON accounts (address, changed_at DESC);

-- Handle account-specific events like initialize and setauthority
CREATE FUNCTION account_handler(inst chainsync.SvmInstruction, job JSONB) RETURNS VOID
AS $$
DECLARE
  discriminator SMALLINT;
  account TEXT;
  owner TEXT;
  mint TEXT;
  closed BOOLEAN = FALSE;
BEGIN
  discriminator := get_byte(inst.data, 0);
  IF discriminator = 1 THEN -- InitializeAccount
    IF inst.accounts[2] <> (job->>'token')::TEXT THEN RETURN; END IF;
    account := inst.accounts[1];
    owner := inst.accounts[3];
  ELSIF discriminator = 18 THEN -- InitializeAccount3
    IF inst.accounts_mints[1] <> (job->>'token')::TEXT THEN RETURN; END IF;
    account := inst.accounts[1];
    owner := base58.encode(SUBSTRING(inst.data FROM 2));
  ELSIF discriminator = 6 THEN -- SetAuthority
    -- We are safe to skip this set authority as it cannot happen for uninitialized accounts
    account := inst.accounts[1];
    owner := inst.accounts[2];
  ELSIF discriminator = 9 THEN -- CloseAccount
    IF inst.accounts_mints[1] <> (job->>'token')::TEXT THEN RETURN; END IF;
    account := inst.accounts[1];
    owner := inst.accounts[3];
    closed := TRUE;
  END IF;

  INSERT INTO accounts (address, owner, mint, signature, changed_at, closed)
  VALUES(account, owner, (job->>'token')::TEXT, inst.signature, TO_TIMESTAMP(inst.block_time) AT TIME ZONE 'UTC', closed)
  ON CONFLICT (address, changed_at) DO UPDATE SET signature = EXCLUDED.signature, owner = EXCLUDED.owner, closed = EXCLUDED.closed;
END;
$$ LANGUAGE plpgsql;

-- This task will build the accounts history table with the accounts that are related to the token
SELECT chainsync.register(
  'svm-accounts',
  '{
    "ws": "...",
    "rpc": "...",
    "oneshot": true,
    "preload": true,

    "svm": {
      "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "mentions": ["..."],
      "instruction_handler": "account_handler",
      "instruction_discriminators": [1, 6, 9, 18]
    },

    "token": "..."
  }'::JSONB
);
