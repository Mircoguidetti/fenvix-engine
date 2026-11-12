"""qprobe.py BIN TAG PROBE [args...]: start our server (replica-set mode) and run a review probe against it and mongod."""
import subprocess, sys
sys.path.insert(0, "/home/user/wt-q/ctl-tests")
import lib
from lib import Server, make_bucket, free_port
lib.OUT = "/tmp/claude-0"
binary, tag, probe = sys.argv[1], sys.argv[2], sys.argv[3]
P = "/home/user/fenvix-brain/sessions/2026-11-11-review-fix-query-limits/probes/"
port = free_port()
env = {"FENVIX_SEALED_TPIN_MS": "30000"}
s = Server(make_bucket("qprobe"), "qprobe", idle=600, port=port, binary=binary, env=env,
           extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
try:
    ours = f"mongodb://127.0.0.1:{port}/?directConnection=true"
    real = "mongodb://127.0.0.1:27020/?directConnection=true"
    r = subprocess.run([sys.executable, P + probe, "--real", real, "--ours", ours] + sys.argv[4:], capture_output=True, text=True, timeout=3600)
    out = r.stdout + r.stderr
    open(f"/home/user/runs/{tag}-{probe}.txt", "w").write(out)
    print("\n".join(l[:200] for l in out.splitlines() if l.startswith("DIFF") or "different" in l or "Error" in l or "Traceback" in l))
finally:
    s.term()
