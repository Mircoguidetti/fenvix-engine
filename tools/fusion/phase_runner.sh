#!/usr/bin/env bash
# Run every verification phase of the fused binary in order; each phase is resumable, so after a machine restart the
# same command continues. Stops at the first phase that is interrupted.
cd "$(dirname "$0")/../.."
O=mongo-tests/out/fusion; mkdir -p $O
{
python3 tools/fusion/run_suites.py 150 && echo "PHASE suites done" || exit 1
tools/fusion/run_ctl.sh && echo "PHASE ctl done" || exit 1
tools/fusion/run_kill.sh && echo "PHASE kill done" || exit 1
tools/fusion/run_wal300.sh && echo "PHASE wal300 done" || exit 1
touch /tmp/claude-0/fusion_all_done
} >> $O/phases.log 2>&1
