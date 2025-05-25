-- Transaction handler for Solana
CREATE FUNCTION custom_svm_transaction_handler(tx chainsync.SvmTransaction, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Processing transaction %', tx.signature;
END;
$$ LANGUAGE plpgsql;

-- Instruction handler for Solana
CREATE FUNCTION custom_svm_instruction_handler(inst chainsync.SvmInstruction, job JSONB) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Processing instruction [%] #%.%: data: %', inst.program_id, inst.index, COALESCE(inst.inner_index, 0), inst.data;
END;
$$ LANGUAGE plpgsql;

-- Listen for transactions
-- Provide your own ws key...
SELECT chainsync.register(
  'svm-listen-transactions',
  '{
    "rpc": "...",
    "ws": "...",

    "svm": {
      "mentions": ["..."],
      "program": "...",
      "transaction_handler": "svm_transaction_handler",
      "instruction_handler": "svm_instruction_handler"
    }
  }'::JSONB
);
