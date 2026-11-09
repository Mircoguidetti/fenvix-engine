"""Step 9 local mode · after local-run.sh: the checks that say the round ran end to end, from the files it left.
  - run.json: outcome FINITO; every phase reached; the SPESA line printed at least once a minute of the runner;
  - log: three log nodes in three zones, ready; the cook and the spare started WITH the log (--quaderno);
  - m30: the client's load, "PLANS: SAME", the program on cook 1, the reads against the plan, the founder's script md5;
  - extras: each program's summary exists and passes its own check (sealed run 0 wrong, takeovers PASS ...);
  - teardown: the final check said "RIMASTO: niente".
Prints one line per check and "LOCAL ROUND: PASS" or "LOCAL ROUND: FAIL".
Usage: check_local.py WORK LOAD_JOB BALLAST_JOB MEASURE_JOB   (the round names of local-run.sh)"""
import glob
import json
import os
import re
import sys

W, load, ballast, name = sys.argv[1:5]
out = os.path.join(W, f"out-{name}")
fails = []


def check(ok, what):
    print(f"{'OK  ' if ok else 'FAIL'} {what}")
    if not ok:
        fails.append(what)


def read(p, o=None):
    try:
        return open(os.path.join(o or out, p), errors="replace").read()
    except OSError:
        return ""


def runjson(n):
    p = os.path.join(W, f"out-{n}", "run.json")
    return json.load(open(p)) if os.path.exists(p) else {}


# job A: the Atlas databases, pantry kept
ra = runjson(load)
check(ra.get("outcome") == "FINITO" and ra.get("outcomes", {}).get("load") == "LOADED", f"job A (Atlas databases): {ra.get('outcome')} {ra.get('outcomes')}")
oa = os.path.join(W, f"out-{load}")
check("load finished with code 0" in read("client/steps.log", oa), "job A: the client's load finished with code 0")
check("loads in this run: 1" in open(os.path.join(W, f"packages-{load}.txt")).read(), "job A: client package loads (LOAD=1)")
# job B: the ballast into the kept pantry
rb = runjson(ballast)
check(rb.get("outcome") == "FINITO" and rb.get("outcomes", {}).get("ballast") == "BALLAST_LOADED", f"job B (ballast): {rb.get('outcome')} {rb.get('outcomes')}")
check("BALLAST: DONE" in read("filler/steps.log", os.path.join(W, f"out-{ballast}")), "job B: filler BALLAST: DONE")
# job C: the measured kitchen
run = runjson(name)
check(run.get("outcome") == "FINITO", f"job C outcome {run.get('outcome')} ({json.dumps(run.get('outcomes'))})")
for r, n in ((ra, "A"), (rb, "B"), (run, "C")):
    peak = r.get("command", {}).get("vcpu_peaks")
    check(bool(peak), f"job {n}: vCPU at once by phase {peak} (local types; on AWS the same command is at most 8)")
log = open(os.path.join(W, f"run-{name}.log")).read() if os.path.exists(os.path.join(W, f"run-{name}.log")) else ""
spesa = re.findall(r"SPESA: ([0-9.]+) \$ so far at (\d+) minutes", log)
check(len(spesa) >= 3, f"SPESA lines: {len(spesa)}, last {spesa[-1] if spesa else None}")
zones = sorted(json.loads(read(f"log-{i}/ready.json") or "{}").get("az", "?") for i in range(3))
check(zones == ["eu-west-1a", "eu-west-1b", "eu-west-1c"], f"log copies ready in zones {zones}")
check(json.loads(read("log-0/ready.json") or "{}").get("ip") == json.loads(read("server-1/ready.json") or "{}").get("ip"),
      "log-0 runs on the cook's machine (same private IP)")
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
ag = read("agent/steps.log")
line = [l for l in ag.splitlines() if "program agent-bench:" in l]
check(bool(line) and "code 0" in line[-1], f"agent-bench (our cook, Postgres machine): {line[-1][21:200] if line else 'not run'}")
for t in ("fenvix", "postgres"):
    sm = read(f"agent/agent-bench/{t}/summary.json")
    check(bool(sm), f"agent-bench {t}: summary.json {'present' if sm else 'missing'}")
check(json.loads(read("pgbox/ready.json") or "{}").get("port") == 5432, "pgbox: PostgreSQL ready on 5432")
check(all(os.path.exists(os.path.join(W, f"teardown-{n}.txt")) for n in (load, ballast, name)), "teardown ran after every job")
print(f"LOCAL ROUND: {'PASS' if not fails else 'FAIL (' + str(len(fails)) + ')'}")
sys.exit(0 if not fails else 1)
