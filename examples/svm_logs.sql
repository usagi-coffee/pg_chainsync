CREATE TABLE logs (
  slot BIGINT NOT NULL,
  number BIGINT PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION svm_log_handler(log chainsync.SvmLog, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Processing log %', log;
END;
$$ LANGUAGE plpgsql;

-- Listen for logs
-- Provide your own ws key...
SELECT chainsync.register(
  'svm-simple-logs',
  '{
    "ws": "...",

    "svm": {
      "mentions": ["..."],
      "program": "...",
      "log_handler": "svm_log_handler"
    }
  }'::JSONB
);
