CREATE FUNCTION custom_block_handler(block chainsync.Block, job_id bigint) RETURNS VOID
AS $$
BEGIN
  RAISE LOG 'Got block % from job %', block.number, job_id;
END;
$$
LANGUAGE plpgsql;

SELECT chainsync.add_blocks_job(31337, 'ws://pg-chainsync-foundry:8545', 'custom_block_handler');
