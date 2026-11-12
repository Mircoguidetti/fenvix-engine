"""qperf.py BUCKET BIN LABEL [--load]: our server on BUCKET (sealed runs on, product defaults), then perf.py."""
import subprocess, sys
sys.path.insert(0, "/home/user/wt-q/ctl-tests")
import lib
from lib import Server
lib.OUT = "/tmp/claude-0"
bucket, binary, label = sys.argv[1], sys.argv[2], sys.argv[3]
P = "/home/user/fenvix-brain/sessions/2026-11-11-review-fix-query-limits/probes/"
s = Server(bucket, "qperf", idle=600, binary=binary).start()
try:
    args = [sys.executable, P + "perf.py", "--ours", s.uri, "--label", label, "--out", f"/home/user/runs/perf-{label}.json"] + sys.argv[4:]
    r = subprocess.run(args, capture_output=True, text=True, timeout=7200)
    open(f"/home/user/runs/perf-{label}.txt", "w").write(r.stdout + r.stderr)
    print(r.stdout[-6000:], r.stderr[-1500:])
finally:
    s.term()
