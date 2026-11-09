"""Step 9 · program "agent-bench": the agent checkpoint benchmark (bench/agent-checkpoints, Part B) on the real
network, from the agent client, against each target one after the other, with the same seeded workload:
  fenvix    our measured cook (3-zone log), through the LangGraph MongoDB saver (`targets.fenvix_saver`);
  postgres  the pgbox machine: PostgreSQL 16 on one EC2 with gp3, the stand-in for a Supabase project (founder,
            2026-11-09: no Supabase account), through the LangGraph Postgres saver;
  aurora    only if the run bucket holds `secret/aurora-uri` (an Aurora cluster made outside this workflow: it needs RDS
            permissions this workflow does not have).
No Atlas (founder, 2026-11-09): the MongoDB-saver side is priced from the local mongod measurements already taken.
Each target's raw timings and summary go to OUT/<target>/ (bench.py's own files). Exit code 0 if every target ran.
Usage: agent_bench.py OUT --fenvix-uri URI --pg-host IP [--profile standard] [--workers 1]   (CM_PW in the environment)"""
import argparse
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
a, extra = ap.parse_known_args()
here = os.path.dirname(os.path.abspath(__file__))
ab = os.path.join(here, "agentbench")
env = dict(os.environ, PYTHONPATH=f"{os.path.join(ab, 'vendor')}:{ab}")
pw = urllib.parse.quote(os.environ["CM_PW"], safe="")
targets = [("fenvix", "fenvix", a.fenvix_uri, "our kitchen: cook m7i.large with the 3-zone log, eu-west-1")]
if a.pg_host:
    targets.append(("postgres", "postgres", f"postgresql://app_user:{pw}@{a.pg_host}:5432/app_demo?sslmode=disable",
                    "PostgreSQL 16 on one EC2 with gp3 (Supabase stand-in), eu-west-1"))
if a.aurora_uri:
    targets.append(("aurora", "postgres", a.aurora_uri, "Aurora PostgreSQL, eu-west-1"))
ok = True
for name, kind, uri, label in targets:
    out = os.path.join(a.out, name)
    os.makedirs(out, exist_ok=True)
    cmd = [sys.executable, os.path.join(ab, "bench.py"), "--target", kind, "--label", label, "--profile", a.profile,
           "--workers", str(a.workers), "--out", out] + extra
    with open(os.path.join(out, "terminal.txt"), "w") as t:
        e = dict(env, BENCH_URI=uri)
        if name == "fenvix" and a.s3_bucket:
            e.update(BENCH_S3_BUCKET=a.s3_bucket, BENCH_S3_REGION="eu-west-1", BENCH_S3_ENDPOINT=os.environ.get("AWS_ENDPOINT_URL", ""))
        rc = subprocess.run(cmd, env=e, stdout=t, stderr=subprocess.STDOUT).returncode
    print(f"target {name}: code {rc}", flush=True)
    ok = ok and rc == 0
print(f"AGENT-BENCH: {'DONE' if ok else 'FAILED'} ({', '.join(t[0] for t in targets)})")
sys.exit(0 if ok else 1)
