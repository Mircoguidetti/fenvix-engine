"""Step 9 · the local benchmark report the gate needs (gate.py): one file per engine commit,
step9/local/benchmark-report-<commit>.json, naming the local takeover result and the local agent benchmark made on that
commit, with the gate's own verdict on them. Run it after both local runs on the final merged engine ref, then commit
the report and the result files on the branch the workflow runs from.
Usage: benchmark_report.py ENGINE_REF ENGINE_COMMIT TAKEOVERS_SUMMARY_JSON AGENT_FENVIX_SUMMARY_JSON
  (paths relative to the repository root, e.g. step9/local/results-2026-11-20/takeovers-summary-on.json and
   bench/agent-checkpoints/results/local-2026-11-20/fenvix/summary.json)
The commit must be the full 40-character id the binaries were built from (git rev-parse HEAD in the engine checkout)."""
import datetime as dt
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(HERE, "..", "ci"))
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
import gate  # noqa: E402

ref, commit, tk, ab = sys.argv[1:5]
if not re.match(r"^[0-9a-f]{40}$", commit):
    sys.exit("ENGINE_COMMIT: the full 40-character commit id")
for p in (tk, ab):
    if not os.path.exists(os.path.join(ROOT, p)):
        sys.exit(f"no such file: {p}")
gate.ROOT = ROOT
blockers = gate.check_takeovers(os.path.join(ROOT, tk)) + gate.check_agent(os.path.join(ROOT, ab))
r = {"engine_ref": ref, "engine_commit": commit, "made": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
     "takeovers": tk, "agent_bench": ab, "verdict": "FAIL" if blockers else "PASS", "blockers": blockers}
out = os.path.join(HERE, f"benchmark-report-{commit[:12]}.json")
json.dump(r, open(out, "w"), indent=1)
print(f"{os.path.relpath(out, ROOT)}: {r['verdict']}" + "".join(f"\n  - {b}" for b in blockers))
sys.exit(0 if not blockers else 1)
