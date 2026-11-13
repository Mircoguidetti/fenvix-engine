"""Step 9 · the cold phase on AWS (total review B9: the AWS agent benchmark had no cold command, so AWS gave no cold
number, the one number only AWS can measure). Tested here, $0, no AWS:
  - cold_restart.py (the client side, bench.py's --cold-cmd) against a moto S3 bucket, with a thread playing the
    machine: it writes the request, waits for the answer, and fails when no answer comes;
  - common.sh serve_cold_restart (the machine side, in cook.sh and pgbox.sh) with s3io.py replaced by a folder: on a
    request it stops, drops the page cache (a stub here), starts, answers ok; the same request is not served twice;
  - agent_bench.py passes the cold command for our cook and the Postgres machine, none for Aurora, and --settle and
    the binary's hash for ours.
Usage: python3 step9/local/tests/test_cold_restart.py"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(S9, "programs"))


def test_requester_waits_for_the_machine():
    import boto3
    from moto import mock_aws
    import cold_restart
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-1")
        s3.create_bucket(Bucket="run", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})

        def machine():
            for _ in range(100):
                try:
                    rid = s3.get_object(Bucket="run", Key="server-1/restart-request")["Body"].read().decode()
                    time.sleep(0.3)
                    s3.put_object(Bucket="run", Key=f"server-1/restarted-{rid}", Body=b'{"ok": true, "drop_caches": "done"}')
                    return
                except Exception:  # noqa: BLE001
                    time.sleep(0.05)
        t = threading.Thread(target=machine)
        t.start()
        r = cold_restart.restart(s3, "run", "server-1", wait_s=20, poll_s=0.1)
        t.join()
        assert r["ok"] is True and r["drop_caches"] == "done", r
        r2 = cold_restart.restart(s3, "run", "pgbox", wait_s=1, poll_s=0.1)   # nobody answers
        assert r2["ok"] is False and "no answer" in r2["error"], r2


FAKE_S3IO = r'''import os, shutil, sys
op, bucket, key = sys.argv[1], sys.argv[2], sys.argv[3]
root = os.environ["FAKE_BUCKET_DIR"]
p = os.path.join(root, key)
if op == "exists":
    sys.exit(0 if os.path.exists(p) else 1)
if op == "text":
    print(open(p).read())
elif op == "put":
    os.makedirs(os.path.dirname(p), exist_ok=True)
    shutil.copy(sys.argv[4], p)
'''


def test_machine_serves_a_request_once():
    d = tempfile.mkdtemp()
    b = os.path.join(d, "bucket")
    os.makedirs(os.path.join(b, "server-1"))
    open(os.path.join(d, "s3io.py"), "w").write(FAKE_S3IO)
    open(os.path.join(b, "server-1", "restart-request"), "w").write("abc123")
    script = f'''set -u
cd {d}; BUCKET=run; P=server-1; PY={sys.executable}
source {S9}/machine/common.sh
stopf() {{ echo stop >> calls; }}
startf() {{ echo start >> calls; return 0; }}
serve_cold_restart stopf startf
serve_cold_restart stopf startf
'''
    r = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                       env=dict(os.environ, FAKE_BUCKET_DIR=b, DROP_CACHES_CMD="echo dropped >> calls"))
    assert r.returncode == 0, r.stdout + r.stderr
    assert open(os.path.join(d, "calls")).read().split() == ["stop", "dropped", "start"], r.stdout
    ans = json.load(open(os.path.join(b, "server-1", "restarted-abc123")))
    assert ans["ok"] is True and ans["drop_caches"] == "done", ans


def test_agent_bench_commands():
    """agent_bench.py builds bench.py's command per target: cold for ours and the Postgres machine, not for Aurora."""
    d = tempfile.mkdtemp()
    fake = os.path.join(d, "fake_python.sh")
    open(fake, "w").write(f'#!/bin/bash\nif [ "${{1##*/}}" = bench.py ]; then echo "$@" >> {d}/calls; exit 0; fi\nexec {sys.executable} "$@"\n')
    os.chmod(fake, 0o755)
    code = (f"import sys,runpy; sys.executable={fake!r}; "
            f"sys.argv=['agent_bench.py','{d}/out','--fenvix-uri','mongodb://x','--pg-host','10.0.0.9','--aurora-uri','postgresql://a',"
            f"'--done-bucket','run','--engine-sha256','{'e' * 64}']; runpy.run_path('{S9}/programs/agent_bench.py', run_name='__main__')")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=dict(os.environ, CM_PW="x"))
    calls = open(os.path.join(d, "calls")).read().splitlines()
    by = {c.split("--target ")[1].split()[0] + ("-aurora" if "Aurora" in c else ""): c for c in calls}
    assert "cold_restart.py run server-1" in by["fenvix"] and "--settle" in by["fenvix"] and "e" * 64 in by["fenvix"], by["fenvix"]
    pg = [c for k, c in by.items() if k == "postgres"][0]
    assert "cold_restart.py run pgbox" in pg, pg
    assert "--cold-cmd" not in by["postgres-aurora"], by["postgres-aurora"]
    assert "no cold phase" in r.stdout, r.stdout


if __name__ == "__main__":
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        fn()
        print(f"PASS {name}")
    print("ALL PASS")
