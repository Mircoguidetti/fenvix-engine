"""Step 9 · the local benchmark report the gate needs (gate.py): one file per engine commit,
step9/local/benchmark-report-<commit>.json, naming the local takeover result and the local agent benchmark made on that
commit, with the gate's own verdict on them. Run it after both local runs on the final merged engine ref, then commit
the report and the result files on the branch the workflow runs from.
Usage: benchmark_report.py ENGINE_REF ENGINE_COMMIT TAKEOVERS_ON_JSON AGENT_FENVIX_SUMMARY_JSON [AGENT_W8_FENVIX_SUMMARY_JSON]
         --takeovers-off TAKEOVERS_OFF_JSON --engine-sha256 SHA256_OF_MONGO_SERVER
  (both flags are required for a PASS: total review N3, the log-off takeovers; and the binary every result names)
  (the fifth: the --workers 8 line of the same benchmark, checked by the gate with the same rules; REQUIRED by the gate since
  the total review R3, 2026-11-12: a report without it is CLOSED)
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

import argparse  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("ref")
ap.add_argument("commit")
ap.add_argument("takeovers", help="takeover summary, log on")
ap.add_argument("agent", help="our summary of the equal-durability benchmark (repeat 1)")
ap.add_argument("w8", nargs="?", default=None, help="our summary of the --workers 8 line")
ap.add_argument("--takeovers-off", default=None, help="takeover summary, log off (total review N3: both are checked)")
ap.add_argument("--engine-sha256", default=None, help="sha256 of the mongo-server binary built from the commit; every "
                "result must carry the same (total review, risk under N5)")
a = ap.parse_args()
ref, commit, tk, ab, w8 = a.ref, a.commit, a.takeovers, a.agent, a.w8
if not re.match(r"^[0-9a-f]{40}$", commit):
    sys.exit("ENGINE_COMMIT: the full 40-character commit id")
if a.engine_sha256 and not re.match(r"^[0-9a-f]{64}$", a.engine_sha256):
    sys.exit("--engine-sha256: 64 hex characters")
for p in [x for x in (tk, ab, w8, a.takeovers_off) if x]:
    if not os.path.exists(os.path.join(ROOT, p)):
        sys.exit(f"no such file: {p}")
gate.ROOT = ROOT
r = {"engine_ref": ref, "engine_commit": commit, "engine_sha256": a.engine_sha256,
     "made": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
     "takeovers": tk, "takeovers_off": a.takeovers_off, "agent_bench": ab, **({"agent_bench_w8": w8} if w8 else {})}
blockers = gate.check_report_results(r)
r |= {"verdict": "FAIL" if blockers else "PASS", "blockers": blockers}
out = os.path.join(HERE, f"benchmark-report-{commit[:12]}.json")
json.dump(r, open(out, "w"), indent=1)
print(f"{os.path.relpath(out, ROOT)}: {r['verdict']}" + "".join(f"\n  - {b}" for b in blockers))
sys.exit(0 if not blockers else 1)
