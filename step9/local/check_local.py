"""Step 9 local mode · after local-run.sh: the checks that say the round ran end to end, from the files it left.
  - run.json: outcome FINITO; every phase reached; the SPESA line printed at least once a minute of the runner;
  - log: three log nodes in three zones, ready; the cook and the spare started WITH the log (--quaderno);
  - m30: the client's load, "PLANS: SAME", the program on cook 1, the reads against the plan, the founder's script md5;
  - extras: each program's summary exists and passes its own check (sealed run 0 wrong, takeovers PASS ...);
  - teardown: the final check said "RIMASTO: niente".
Prints one line per check and "LOCAL ROUND: PASS" or "LOCAL ROUND: FAIL".
Usage: check_local.py WORK NAME"""
import glob
import json
import os
import re
import sys

W, name = sys.argv[1], sys.argv[2]
out = os.path.join(W, f"out-{name}")
fails = []


def check(ok, what):
    print(f"{'OK  ' if ok else 'FAIL'} {what}")
    if not ok:
        fails.append(what)


def read(p):
    try:
        return open(os.path.join(out, p), errors="replace").read()
    except OSError:
        return ""


run = json.load(open(os.path.join(out, "run.json"))) if os.path.exists(os.path.join(out, "run.json")) else {}
check(run.get("outcome") == "FINITO", f"run outcome {run.get('outcome')} ({json.dumps(run.get('outcomes'))})")
log = open(os.path.join(W, f"run-{name}.log")).read() if os.path.exists(os.path.join(W, f"run-{name}.log")) else ""
spesa = re.findall(r"SPESA: ([0-9.]+) \$ so far at (\d+) minutes", log)
check(len(spesa) >= 3, f"SPESA lines: {len(spesa)}, last {spesa[-1] if spesa else None}")
zones = sorted(json.loads(read(f"log-{i}/ready.json") or "{}").get("az", "?") for i in range(3))
check(zones == ["eu-west-1a", "eu-west-1b", "eu-west-1c"], f"log nodes ready in zones {zones}")
for n in ("server-1", "spare"):
    c = read(f"{n}/server-command.txt")
    check("--quaderno" in c, f"{n} started with the 3-zone log: {'yes' if '--quaderno' in c else 'no'}")
steps = read("client/steps.log")
check("PLANS: SAME" in steps, "client: PLANS: SAME (same clients and windows as the fake Atlas)")
check("script and code-irlanda files: originals" in steps, "client: founder's script and code-irlanda files unchanged (md5)")
m = re.search(r"size 1 \(([^)]*)\): (\S+);", steps)
check(bool(m) and m.group(2).startswith("FINITO"), f"client: m30 program on cook 1: {m.groups() if m else 'not found'}")
reads = read("client/size-1/reads.txt").strip().splitlines()
check(bool(reads) and "ONE TO ONE" in reads[-1].upper(), f"client: reads against the plan: {reads[-1] if reads else 'none'}")
filler = read("filler/steps.log")
check("BALLAST: DONE" in filler, f"filler: {[l for l in filler.splitlines() if 'BALLAST' in l][-1:]}")
ex = read("extras/steps.log")
for prog in ("s3-first-byte", "sealed-run", "log-ack", "ballast-rounds", "takeovers"):
    line = [l for l in ex.splitlines() if f"program {prog}:" in l]
    check(bool(line) and "code 0" in line[-1], f"extras {prog}: {line[-1][21:200] if line else 'not run'}")
s = read("extras/sealed-run/sealed-run-summary.json")
check(bool(s) and json.loads(s).get("wrong") == 0, f"sealed run: wrong rows {json.loads(s).get('wrong') if s else None}")
t = read("extras/takeovers/takeovers-summary.json")
tj = json.loads(t) if t else {}
check(tj.get("verdict") == "PASS", f"takeovers: {tj.get('verdict')}, {tj.get('done')}/{tj.get('n')} done, acknowledged "
      f"{tj.get('acknowledged')}, lost {tj.get('lost')}, confirmed after a kill {tj.get('confirmed_after_kill')}, "
      f"p50 {tj.get('takeover_s_p50')} s")
fin = [l for l in log.splitlines()]
check(os.path.exists(os.path.join(W, f"teardown-{name}.txt")), "teardown ran")
print(f"LOCAL ROUND: {'PASS' if not fails else 'FAIL (' + str(len(fails)) + ')'}")
sys.exit(0 if not fails else 1)
