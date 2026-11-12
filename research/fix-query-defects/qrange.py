import subprocess, sys
sys.path.insert(0, "/home/user/wt-q/ctl-tests")
import lib
from lib import Server, make_bucket
lib.OUT = "/tmp/claude-0"
binary, label = sys.argv[1], sys.argv[2]
s = Server(make_bucket("qrange"), "qrange", idle=600, binary=binary).start()
try:
    r = subprocess.run([sys.executable, "/home/user/fenvix-brain/sessions/2026-11-11-review-fix-query-limits/probes/range_ab.py", s.uri, "app_range", "200000"], capture_output=True, text=True, timeout=3600)
    print("\n".join(f"{label:5} {l}" for l in (r.stdout + r.stderr[-600:]).splitlines()))
finally:
    s.term()
