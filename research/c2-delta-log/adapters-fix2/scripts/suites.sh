#!/usr/bin/env bash
export FENVIX_SEALED_COMPACTOR=small FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1
export OURS_EXTRA="--sealed-runs 2 --sealed-compactor --sealed-derive --sealed-codec zstd --customer-key checkpoints=thread_id,checkpoint_writes=thread_id"
/tmp/claude-0/-home-user/9129b5ca-4194-55a1-8a84-bd2fce2b2dd9/scratchpad/fix2/bench/agent-adapters/run_suites.sh ours lg-py-saver lg-py-store lg-js mastra 2>&1 | grep -v "not a git"
/tmp/claude-0/-home-user/9129b5ca-4194-55a1-8a84-bd2fce2b2dd9/scratchpad/fix2/bench/agent-adapters/target.sh stopall
