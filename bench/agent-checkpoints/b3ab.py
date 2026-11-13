"""P-B3 step 3 (2026-11-13): A/B in one run on one loaded line, as a mix round's command (lines.py run ... --rounds).

  b3ab.py base     the cook and the spare run the base binary (B3_BASE_BIN): compaction inside the cook, as today
  b3ab.py remote   the cook and the spare run the B3 binary (B3_NEW_BIN) with --sealed-remote-compactor; a third
                   container "compactor" (1 CPU and the same memory cap as every member, its own process: the "other
                   machine") runs `mongo-server --compactor-only app_demo` with the same server flags
Each switch: the compactor container is stopped first (SIGTERM: it releases its claim), the cook and the spare are
recreated with the binary (recook.py's way: same bucket, same log copies, no data touched), then the caller's
`lines.py restart` makes the round cold; `b3ab.py post MODE` starts the compactor container after that restart.
Every action goes to $B3_LOG.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

H = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, H)
import lines  # noqa: E402

LOG = os.environ.get("B3_LOG", "/tmp/fxbench/b3ab.jsonl")
lines.IP["compactor"] = "172.31.77.46"


def log(**kw) -> None:
    kw = {"t": round(time.time(), 3), **kw}
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a") as f:
        f.write(json.dumps(kw) + "\n")
    print(json.dumps(kw), flush=True)


def cpu_ns(name: str) -> int | None:
    cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", name], capture_output=True, text=True).stdout.strip()
    try:
        return int(open(f"/sys/fs/cgroup/cpuacct/docker/{cid}/cpuacct.usage").read())
    except (OSError, ValueError):
        return None


def stop_compactor() -> None:
    log(action="cpu before the switch", cook_ns=cpu_ns("cook"), compactor_ns=cpu_ns("compactor"))
    r = subprocess.run(["docker", "stop", "-t", "60", "compactor"], capture_output=True, text=True)
    if r.returncode == 0:
        logs = subprocess.run(["docker", "logs", "--tail", "4", "compactor"], capture_output=True, text=True)
        log(action="compactor stopped", tail=(logs.stdout + logs.stderr)[-1500:])
        out = os.environ.get("B3_COMPACTOR_LOGS")
        if out:
            full = subprocess.run(["docker", "logs", "compactor"], capture_output=True, text=True)
            with open(os.path.join(out, f"compactor-{int(time.time())}.log"), "w") as f:
                f.write(full.stdout + full.stderr)
    subprocess.run(["docker", "rm", "-f", "compactor"], capture_output=True)


def recook(binary: str, flags: str) -> None:
    lines.set_big(os.environ.get("B3_MEM", "1g"))
    os.environ["FXBENCH_SERVER_FLAGS"] = flags
    for n in ("spare", "cook"):
        subprocess.run(["docker", "stop", "-t", "120", n], capture_output=True)
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    os.remove(lines.SRV)
    shutil.copy2(binary, lines.SRV)
    lines.run_ours("cook", lines.server_cmd("cook", True))
    lines.run_ours("spare", lines.server_cmd("spare", True))
    lines.wait_until("our server answers as root", lambda: lines.ours_ready("fenvix-log"), 300)
    lines.wait_until("spare listening", lambda: "listening on" in lines.docker_logs("spare"), 120)
    log(action="recooked", binary=binary, flags=flags, sha256=lines.bin_sha()[lines.SRV][:16])


def start_compactor() -> None:
    lines.set_big(os.environ.get("B3_MEM", "1g"))
    os.environ["FXBENCH_SERVER_FLAGS"] = os.environ.get("B3_FLAGS", "--box-disk-mb 2048")
    cmd = lines.server_cmd("compactor", False) + ["--compactor-only", "app_demo"]
    lines.run_ours("compactor", cmd)
    t0 = time.time()
    while time.time() - t0 < 120:
        txt = lines.docker_logs("compactor")
        if "compactor started" in txt:
            log(action="compactor running", seconds=round(time.time() - t0, 1), head=txt[-600:])
            return
        if subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", "compactor"], capture_output=True, text=True).stdout.strip() != "true":
            raise SystemExit(f"the compactor container stopped: {txt[-2000:]}")
        time.sleep(1)
    raise SystemExit(f"the remote compactor did not start in 120 s: {lines.docker_logs('compactor')[-2000:]}")


def main() -> None:
    mode = sys.argv[1]
    flags = os.environ.get("B3_FLAGS", "--box-disk-mb 2048")
    if mode == "base":
        stop_compactor()
        recook(os.environ["B3_BASE_BIN"], flags)
    elif mode == "remote":
        stop_compactor()
        recook(os.environ["B3_NEW_BIN"], flags + " --sealed-remote-compactor")
    elif mode == "post":
        if sys.argv[2] == "remote":
            start_compactor()
        log(action=f"post {sys.argv[2]}", cook_ns=cpu_ns("cook"), compactor_ns=cpu_ns("compactor"))
    elif mode == "stop":
        stop_compactor()
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
