#!/usr/bin/env bash
# Every verification phase of the final binary, in order; each phase is resumable, so after a machine restart the same command
# continues. Stops at the first phase that is interrupted.
cd "$(dirname "$0")/../.."
O=mongo-tests/out/final; mkdir -p $O
{
python3 tools/final/run_suites.py 150 && echo "PHASE suites done" || exit 1
tools/final/run_ctl.sh && echo "PHASE ctl done" || exit 1
tools/final/run_kill.sh && echo "PHASE kill done" || exit 1
tools/final/run_gaps.sh && echo "PHASE gaps done" || exit 1
touch /tmp/claude-0/final_all_done
} >> $O/phases.log 2>&1
