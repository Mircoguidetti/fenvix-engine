#!/bin/bash
# Review of bench-local (2026-11-11). Usage: bash gate_skips.sh REPO   (a fenvix-engine checkout: bench-local-2026-11-11
# or claude/dazzling-bohr-z5zcqd). Three fake result trees, fed to the checkout's own benchmark_report.py and gate.py:
#  case A "failed run": ours ok=false (37 wrong reads, 5 erased threads readable), resume[cold] missing, bytes unknown,
#          every present operation as fast as the others. Must be CLOSED.
#  case B "clean, but slow where the gate does not look": every side ok, every operation present with enough samples,
#          ours 10x slower on resume[write/none] (21,173 samples in the standard plan, the commonest read) and
#          resume[write/short]; equal elsewhere. P-bench-local 4: CLOSED (worse on an operation). Must be CLOSED.
#  case C "only the cold resume is slower": clean, ours 10x slower on resume[cold] only. DECISIONS 2026-11-11: cold is
#          reported, not gated. Must be OPEN.
set -u; R=$1
for case in A B C; do
T=$(mktemp -d); cd "$T"
mkdir -p step9/local/results-x bench/agent-checkpoints/results/local-x/{fenvix,postgres,mongo} step9/ci
cp "$R/step9/ci/gate.py" step9/ci/; cp "$R/step9/local/benchmark_report.py" step9/local/
CASE=$case python3 - <<'P'
import json, os
base="bench/agent-checkpoints/results/local-x/"
n={"put":96754,"put_writes":120000,"save_step":96754,"resume[read]":300,"resume[write/first]":2453,"resume[write/none]":21173,
   "resume[write/short]":1083,"resume[write/long]":689,"resume[cold]":200,"time_travel":900,"list":300,"list_limit10":300,"erase_user":100}
def side(ok=True, slow=(), drop=(), spl=0.5):
    lat={k:{"n":v,"p50":5.0*(10 if k in slow else 1),"p99":20.0*(10 if k in slow else 1)} for k,v in n.items() if k not in drop}
    return {"ok":ok,"latency_ms":lat,"bytes":{"stored_per_logical":spl},"plan":{"digest":"cc5dfd2ec7d93f21"},"wrong_reads":0,
            "write":{"errors":[]},"read":{"inconsistencies":0},"erase":{"threads_left_readable":0}}
them=side()
if os.environ["CASE"]=="A":
    ours=side(ok=False, drop=("resume[cold]",), spl=None); ours["wrong_reads"]=37; ours["erase"]["threads_left_readable"]=5
elif os.environ["CASE"]=="B":
    ours=side(slow=("resume[write/none]","resume[write/short]"))
else:
    ours=side(slow=("resume[cold]",))
for t,d in (("fenvix",ours),("postgres",them),("mongo",them)): json.dump(d,open(base+t+"/summary.json","w"))
json.dump({"verdict":"PASS","lost":0,"done":50,"n":50,"acknowledged":500000},open("step9/local/results-x/takeovers-summary-on.json","w"))
P
C=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
echo "== case $case on $(git -C "$R" rev-parse --short HEAD)"
python3 step9/local/benchmark_report.py final $C step9/local/results-x/takeovers-summary-on.json bench/agent-checkpoints/results/local-x/fenvix/summary.json || true
ENGINE_REF=final ENGINE_COMMIT=$C python3 step9/ci/gate.py . || true
done
