"""Swap the server binary of a kept big line (lines.py run ... --keep): the cook and the spare are recreated with BIN,
on the same MinIO bucket and the same three log copies (no data is touched). Extra env for the cook: COOK_ENV="K=V ..."
(e.g. CPU_PROF_FILE=/diag/prof.folded with COOK_DIR mounted at /diag).

  BENCH_PW=... python recook.py BIN
"""
import os, shutil, subprocess, sys, time
H = os.environ.get("HARNESS", "/home/user/harness/bench/agent-checkpoints")
sys.path.insert(0, H)
import lines  # noqa: E402

b = sys.argv[1]
lines.set_big("1g")
for n in ("spare", "cook"):
    # a clean stop releases the database's lease (a kill leaves it held until it expires)
    subprocess.run(["docker", "stop", "-t", "120", n], capture_output=True)
    subprocess.run(["docker", "rm", "-f", n], capture_output=True)
os.remove(lines.SRV)          # the old file stays alive for nothing; new containers bind the new one
shutil.copy2(b, lines.SRV)
if os.environ.get("COOK_ENV"):
    os.environ["FXBENCH_SERVER_ENV"] = os.environ["COOK_ENV"]
if os.environ.get("COOK_DIR"):
    os.environ["FXBENCH_SERVER_DIR"] = os.environ["COOK_DIR"]
lines.run_ours("cook", lines.server_cmd("cook", True))
os.environ.pop("FXBENCH_SERVER_ENV", None)
os.environ.pop("FXBENCH_SERVER_DIR", None)
lines.run_ours("spare", lines.server_cmd("spare", True))
lines.wait_until("our server answers as root", lambda: lines.ours_ready("fenvix-log"), 300)
lines.wait_until("spare listening", lambda: "listening on" in lines.docker_logs("spare"), 120)
from pymongo import MongoClient  # noqa: E402


def db_answers():
    c = MongoClient(lines.uri_of("fenvix-log"), serverSelectionTimeoutMS=5000)
    c.app_demo.checkpoints.find_one({}, {"_id": 1})
    return True


lines.wait_until("app_demo answers", db_answers, 300)
print("recooked with", b, "sha256", lines.bin_sha()[lines.SRV][:16], flush=True)
