"""Step 9 · program "agent-bench": the agent checkpoint benchmark (bench/agent-checkpoints, Part B) on the real
network, from the agent client, against each target one after the other, with the same seeded workload:
  fenvix    our measured cook (3-zone log), through the LangGraph MongoDB saver (`targets.fenvix_saver`);
  postgres  the pgbox machine: PostgreSQL 16 on one EC2 with gp3, the stand-in for a Supabase project (founder,
            2026-11-09: no Supabase account), through the LangGraph Postgres saver;
  aurora    only if the run bucket holds `secret/aurora-uri` (an Aurora cluster made outside this workflow: it needs RDS
            permissions this workflow does not have).
No Atlas (founder, 2026-11-09): the MongoDB-saver side is priced from the local mongod measurements already taken.
aws-fixes (B10, coordinator's decision 2026-11-11): Aurora is the EQUAL-DURABILITY line on AWS (6 copies in 3 zones, a
write acknowledged at 4 of 6, DECLARED by AWS; ours waits for 2 of 3 log copies in 3 zones). PostgreSQL on the EC2
machine is ONE NODE WITH NO COPY: every output of that target says "single node, no copy" (its label, its summary's
"durability", the line printed here). Order: aurora FIRST (then the run bucket gets agent/aurora-done and run.py deletes
the cluster at once, so it is not paid while the other targets run), then ours, then the Postgres machine. Every
Postgres connection uses TLS (sslmode=require), as ours and Aurora's do. Before each Postgres target its settings are
saved: OUT/<target>/pg_settings.json (SELECT name, setting, unit, source FROM pg_settings, plus version(); on Aurora also
aurora_version()): Aurora's pg_settings shows the values of its parameter groups.
Each target's raw timings and summary go to OUT/<target>/ (bench.py's own files). Exit code 0 if every target ran.
Usage: agent_bench.py OUT --fenvix-uri URI --pg-host IP [--profile standard] [--workers 1]   (CM_PW in the environment)"""
import argparse
import json
import os
import subprocess
import sys
import urllib.parse

ap = argparse.ArgumentParser()
ap.add_argument("out")
ap.add_argument("--fenvix-uri", required=True)
ap.add_argument("--pg-host", default="")
ap.add_argument("--aurora-uri", default="")
ap.add_argument("--s3-bucket", default="", help="our pantry: bench.py reads our stored bytes there (machine role, no keys)")
ap.add_argument("--profile", default="standard")
ap.add_argument("--workers", type=int, default=1)
ap.add_argument("--done-bucket", default="", help="the run bucket: agent/aurora-done is written there after Aurora's target")
ap.add_argument("--done-prefix", default="agent")
a, extra = ap.parse_known_args()
here = os.path.dirname(os.path.abspath(__file__))
ab = os.path.join(here, "agentbench")
env = dict(os.environ, PYTHONPATH=f"{os.path.join(ab, 'vendor')}:{ab}")
pw = urllib.parse.quote(os.environ["CM_PW"], safe="")
DUR = {"fenvix": "equal durability: 3-zone log, a write acknowledged at 2 of 3 copies",
       "aurora": "equal durability: Aurora storage, 6 copies in 3 zones, acknowledged at 4 of 6 (DECLARED by AWS)",
       "postgres": "single node, no copy (NOT equal durability: one EC2 machine, one gp3 disk)"}
targets = []
if a.aurora_uri:
    targets.append(("aurora", "postgres", a.aurora_uri, "Aurora PostgreSQL db.r7g.large, eu-west-1a: " + DUR["aurora"]))
targets.append(("fenvix", "fenvix", a.fenvix_uri, "our kitchen: cook m7i.large with the 3-zone log, eu-west-1a: " + DUR["fenvix"]))
if a.pg_host:
    targets.append(("postgres", "postgres", f"postgresql://app_user:{pw}@{a.pg_host}:5432/app_demo?sslmode=require",
                    "PostgreSQL 16 on one EC2 m7i.large with gp3 (Supabase stand-in), eu-west-1a: " + DUR["postgres"]))


def pg_settings(uri, path, aurora):
    """Save the target's settings (B10). Never stops the benchmark: an error is written in the file."""
    rec = {}
    try:
        import psycopg
        with psycopg.connect(uri, autocommit=True) as c:
            rec["version"] = c.execute("select version()").fetchone()[0]
            if aurora:
                try:   # its own guard: the settings below are saved even if this one fails
                    rec["aurora_version"] = c.execute("select aurora_version()").fetchone()[0]
                except Exception as e:  # noqa: BLE001
                    rec["aurora_version_error"] = f"{type(e).__name__}: {str(e)[:120]}"
            rec["ssl_in_use"] = c.execute("select ssl from pg_stat_ssl where pid = pg_backend_pid()").fetchone()[0]
            rec["pg_settings"] = [dict(zip(("name", "setting", "unit", "source"), r)) for r in
                                  c.execute("select name, setting, unit, source from pg_settings order by name").fetchall()]
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
    with open(path, "w") as f:
        json.dump(rec, f, indent=1)
    sb = next((x for x in rec.get("pg_settings", []) if x["name"] == "shared_buffers"), {})
    return f"settings saved (shared_buffers {sb.get('setting')} x {sb.get('unit')}, TLS {rec.get('ssl_in_use')}{', ' + rec['error'] if 'error' in rec else ''})"


def done_marker(name):
    if not a.done_bucket:
        return
    try:
        import boto3
        boto3.client("s3", region_name="eu-west-1").put_object(Bucket=a.done_bucket, Key=f"{a.done_prefix}/{name}-done", Body=b"1")
    except Exception as e:  # noqa: BLE001 - run.py deletes Aurora at the end of the benchmark anyway
        print(f"marker {name}-done: {type(e).__name__}", flush=True)


ok = True
for name, kind, uri, label in targets:
    out = os.path.join(a.out, name)
    os.makedirs(out, exist_ok=True)
    if kind == "postgres":
        print(f"target {name}: {pg_settings(uri, os.path.join(out, 'pg_settings.json'), name == 'aurora')}", flush=True)
    cmd = [sys.executable, os.path.join(ab, "bench.py"), "--target", kind, "--label", label, "--profile", a.profile,
           "--durability", DUR[name], "--workers", str(a.workers), "--out", out] + extra
    if name == "fenvix":
        cmd.append("--settle")   # bytes after compaction 2 has gone idle, as locally (bench-local review, item 8)
    with open(os.path.join(out, "terminal.txt"), "w") as t:
        e = dict(env, BENCH_URI=uri)
        if name == "fenvix" and a.s3_bucket:
            e.update(BENCH_S3_BUCKET=a.s3_bucket, BENCH_S3_REGION="eu-west-1", BENCH_S3_ENDPOINT=os.environ.get("AWS_ENDPOINT_URL", ""))
        rc = subprocess.run(cmd, env=e, stdout=t, stderr=subprocess.STDOUT).returncode
    print(f"target {name} ({DUR[name]}): code {rc}", flush=True)
    if name == "aurora":
        done_marker("aurora")
    ok = ok and rc == 0
print(f"AGENT-BENCH: {'DONE' if ok else 'FAILED'} ({', '.join(t[0] for t in targets)})")
sys.exit(0 if ok else 1)
