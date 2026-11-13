"""Step 9 local mode · after local-run.sh: the checks that say the round ran end to end, from the files it left.
  - run.json: outcome FINITO; every phase reached; the SPESA line printed at least once a minute of the runner;
  - log: three log nodes in three zones, ready; the cook and the spare started WITH the log (--quaderno);
  - m30: the client's load, "PLANS: SAME", the program on cook 1, the reads against the plan, the founder's script md5;
  - extras: each program's summary exists and passes its own check (sealed run 0 wrong, takeovers PASS ...);
  - teardown: the final check said "RIMASTO: niente".
Prints one line per check and "LOCAL ROUND: PASS" or "LOCAL ROUND: FAIL".
aws-fixes (B2): the measure is three jobs (m30; extras and write-peak; agent benchmark), each checked from its own files;
also checked: the settle record of the measured cook says calm (B5), write-peak's summary (B4), the takeover id files
(question 2), every machine's zone.json and the clients in another zone than the cook (question 1), the Postgres
machine's TLS and shared_buffers and its "single node, no copy" label (B10).
Usage: check_local.py WORK LOAD_JOB BALLAST_JOB M30_JOB EXTRAS_JOB AGENT_JOB   (the round names of local-run.sh)"""
import glob
import json
import os
import re
import sys

W, load, ballast, name, xname, aname = sys.argv[1:7]
out = os.path.join(W, f"out-{name}")
xout, aout = os.path.join(W, f"out-{xname}"), os.path.join(W, f"out-{aname}")
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
# aws-ready: the ballast is generated (ballast_mode generate) by the generator machine; the old filler path if not
gen = rb.get("outcomes", {}).get("ballast") == "BALLAST_GENERATED"
check(rb.get("outcome") == "FINITO" and rb.get("outcomes", {}).get("ballast") in ("BALLAST_LOADED", "BALLAST_GENERATED"), f"job B (ballast): {rb.get('outcome')} {rb.get('outcomes')}")
check("BALLAST: DONE" in read("generator/steps.log" if gen else "filler/steps.log", os.path.join(W, f"out-{ballast}")), f"job B: {'generator' if gen else 'filler'} BALLAST: DONE")
# job C: the measured kitchen
run = runjson(name)
check(run.get("outcome") == "FINITO", f"job C1 (m30) outcome {run.get('outcome')} ({json.dumps(run.get('outcomes'))})")
rx, rg = runjson(xname), runjson(aname)
check(rx.get("outcome") == "FINITO", f"job C2 (extras, write-peak) outcome {rx.get('outcome')} ({json.dumps(rx.get('outcomes'))})")
check(rg.get("outcome") == "FINITO", f"job C3 (agent benchmark) outcome {rg.get('outcome')} ({json.dumps(rg.get('outcomes'))})")
for r, n in ((ra, "A"), (rb, "B"), (run, "C1"), (rx, "C2"), (rg, "C3")):
    peak = r.get("command", {}).get("vcpu_peaks")
    check(bool(peak), f"job {n}: vCPU at once by phase {peak} (local types; on AWS the same command is at most 8)")
log = open(os.path.join(W, f"run-{name}.log")).read() if os.path.exists(os.path.join(W, f"run-{name}.log")) else ""
spesa = re.findall(r"SPESA: ([0-9.]+) \$ so far at (\d+) minutes", log)
check(len(spesa) >= 3, f"SPESA lines: {len(spesa)}, last {spesa[-1] if spesa else None}")
zones = sorted(json.loads(read(f"log-{i}/ready.json") or "{}").get("az", "?") for i in range(3))
check(zones == ["eu-west-1a", "eu-west-1b", "eu-west-1c"], f"log copies ready in zones {zones}")
check(json.loads(read("log-0/ready.json") or "{}").get("ip") == json.loads(read("server-1/ready.json") or "{}").get("ip"),
      "log-0 runs on the cook's machine (same private IP)")
for n, o in (("server-1", out), ("spare", xout)):
    c = read(f"{n}/server-command.txt", o)
    check("--quaderno" in c, f"{n} started with the 3-zone log: {'yes' if '--quaderno' in c else 'no'}")
steps = read("client/steps.log")
check("PLANS: SAME" in steps, "client: PLANS: SAME (same clients and windows as the fake Atlas)")
check("script and code-irlanda files: originals" in steps, "client: founder's script and code-irlanda files unchanged (md5)")
m = re.search(r"size 1 \(([^)]*)\): (\S+);", steps)
check(bool(m) and m.group(2).startswith("FINITO"), f"client: m30 program on cook 1: {m.groups() if m else 'not found'}")
st = read("client/size-1/settle.json")
check(bool(st) and json.loads(st).get("calm") is True, f"client: the measured cook settled before the program (B5): "
      f"{json.loads(st).get('reason') if st else 'no settle record'}")
zs = {n: json.loads(read(f"{n}/zone.json") or "{}") for n in ("server-1", "client")}
check(all(z.get("zone_id") for z in zs.values()) and zs["server-1"].get("zone") != zs["client"].get("zone"),
      f"zones recorded, the client in another zone than the cook: {zs}")
reads = read("client/size-1/reads.txt").strip().splitlines()
check(bool(reads) and "ONE TO ONE" in reads[-1].upper(), f"client: reads against the plan: {reads[-1] if reads else 'none'}")
ex = read("extras/steps.log", xout)
for prog in ("s3-first-byte", "sealed-run", "log-ack", "ballast-rounds", "takeovers", "takeovers-logoff"):
    line = [l for l in ex.splitlines() if f"program {prog}:" in l]
    check(bool(line) and "code 0" in line[-1], f"extras {prog}: {line[-1][21:200] if line else 'not run'}")
s = read("extras/sealed-run/sealed-run-summary.json", xout)
check(bool(s) and json.loads(s).get("wrong") == 0, f"sealed run: wrong rows {json.loads(s).get('wrong') if s else None}")
# every program the extras client ran, whatever its name, ended with code 0 (aws-ready 2026-11-10: the log-off takeover
# run failed with code 1 and no check looked at it)
bad = [l[21:200] for l in ex.splitlines() if re.search(r"program \S+: code [1-9]", l)]
check(not bad, f"extras: every program code 0{(': ' + '; '.join(bad)) if bad else ''}")
for lbl in ("", "-logoff"):
    f = read(f"extras/takeovers/takeovers{lbl}-summary.json", xout)
    if lbl and not f:
        check(False, "takeovers with the log off: no summary")
        continue
    if lbl:
        j = json.loads(f)
        check(j.get("verdict") == "PASS", f"takeovers with the log off: {j.get('verdict')}, {j.get('done')}/{j.get('n')} done, lost {j.get('lost')}")
t = read("extras/takeovers/takeovers-summary.json", xout)
tj = json.loads(t) if t else {}
check(tj.get("verdict") == "PASS", f"takeovers: {tj.get('verdict')}, {tj.get('done')}/{tj.get('n')} done, acknowledged "
      f"{tj.get('acknowledged')}, lost {tj.get('lost')}, confirmed after a kill {tj.get('confirmed_after_kill')}, "
      f"p50 {tj.get('takeover_s_p50')} s")
check(os.path.exists(os.path.join(xout, "extras/takeovers/takeovers-acked.jsonl.gz")) and
      bool(read("extras/takeovers/takeovers-missing.json", xout)), "takeovers: every acknowledged id and every missing id saved")
wp = read("peak/write-peak/write-peak-summary.json", xout)
wj = json.loads(wp) if wp else {}
check(bool(wj.get("per_minute")) and wj.get("dropped") and not wj.get("ended_before_time"),
      f"write-peak through the measured cook: steady {wj.get('steady_docs_per_s')}, client CPU {wj.get('client_cpu_pct')}, "
      f"valid {wj.get('valid')} (local: the client shares the machine)")
ag = read("agent/steps.log", aout)
line = [l for l in ag.splitlines() if "program agent-bench:" in l]
check(bool(line) and "code 0" in line[-1], f"agent-bench (our cook, Postgres machine): {line[-1][21:200] if line else 'not run'}")
for t in ("fenvix", "postgres"):
    sm = read(f"agent/agent-bench/{t}/summary.json", aout)
    check(bool(sm), f"agent-bench {t}: summary.json {'present' if sm else 'missing'}")
    if t == "postgres" and sm:
        check("single node, no copy" in json.loads(sm).get("durability", ""), "agent-bench postgres: labelled single node, no copy")
        pgs = json.loads(read("agent/agent-bench/postgres/pg_settings.json", aout) or "{}")
        sb = {x["name"]: x for x in pgs.get("pg_settings", [])}.get("shared_buffers", {})
        check(pgs.get("ssl_in_use") is True and sb.get("setting"), f"agent-bench postgres: TLS in use {pgs.get('ssl_in_use')}, "
              f"shared_buffers {sb.get('setting')} x {sb.get('unit')}")
pr = json.loads(read("pgbox/ready.json", aout) or "{}")
check(pr.get("port") == 5432 and pr.get("tls") == "required", f"pgbox: PostgreSQL ready on 5432, TLS {pr.get('tls')}, shared_buffers {pr.get('shared_buffers_mb')} MB")
check(all(os.path.exists(os.path.join(W, f"teardown-{n}.txt")) for n in (load, ballast, name, xname, aname)), "teardown ran after every job")
print(f"LOCAL ROUND: {'PASS' if not fails else 'FAIL (' + str(len(fails)) + ')'}")
sys.exit(0 if not fails else 1)
