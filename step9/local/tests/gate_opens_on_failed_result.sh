#!/bin/bash
# Review-total, 2026-11-11. Run from a checkout of fenvix-engine aws-ready (9962689): bash gate_opens_on_failed_result.sh REPO
# Builds a fake result tree where OUR summary has ok=false (write errors, 37 inconsistent reads, 5 erased threads still
# readable), save_step and time_travel far slower, stored bytes unknown and most operations missing; the others are fine.
# Expected with today's gate.py: "PASS" and "GATE: OPEN". It must say CLOSED.
# aws-fixes (2026-11-11): copied from fenvix-brain sessions/2026-11-11-review-total/evidence/ unchanged except "|| true"
# after benchmark_report.py, which now says FAIL and exits 1 (set -e would stop before the gate is asked).
# Run by step9/local/tests/test_gate.py, which requires "GATE: CLOSED".
set -e; R=$1; T=$(mktemp -d); cd "$T"
mkdir -p step9/local/results-x bench/agent-checkpoints/results/local-x/{fenvix,postgres,mongo} step9/ci
cp "$R/step9/ci/gate.py" step9/ci/; cp "$R/step9/local/benchmark_report.py" step9/local/
python3 - <<'P'
import json
base="bench/agent-checkpoints/results/local-x/"
ops=["put","put_writes","resume[read]","resume[write/first]","resume[write/long]","list","list_limit10","erase_user","save_step","time_travel"]
them={"latency_ms":{k:{"p50":5.0,"p99":20.0} for k in ops},"bytes":{"stored_per_logical":0.4},"ok":True}
ours={"latency_ms":{"put":{"p50":1.0,"p99":2.0},"save_step":{"p50":900.0,"p99":9000.0},"time_travel":{"p50":500.0,"p99":5000.0}},
      "bytes":{"stored_per_logical":None},"ok":False,"write":{"errors":["boom"]},"read":{"inconsistencies":37},"erase":{"threads_left_readable":5}}
for t,d in (("fenvix",ours),("postgres",them),("mongo",them)): json.dump(d,open(base+t+"/summary.json","w"))
json.dump({"verdict":"PASS","lost":0,"done":50,"n":50,"acknowledged":500000},open("step9/local/results-x/takeovers-summary-on.json","w"))
P
C=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
python3 step9/local/benchmark_report.py final $C step9/local/results-x/takeovers-summary-on.json bench/agent-checkpoints/results/local-x/fenvix/summary.json || true
ENGINE_REF=final ENGINE_COMMIT=$C python3 step9/ci/gate.py .
