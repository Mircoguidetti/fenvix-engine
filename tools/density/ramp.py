"""Scenario 4 (memory limit): fresh engine, then STEP more databases every STEP_S seconds, each kept open by one
request every PERIOD seconds (PERIOD < --idle-secs, so they never close), until the engine's RSS passes LIMIT_MB or
MAX_N databases are open. Same sampler as the other runs.
usage: ramp.py NAME [--step 250] [--step-s 180] [--period 20] [--max-n 2000] [--limit-mb 3072] [--hold-s 180]"""
import argparse
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def rss_mb(pid):
    with open(f"/proc/{pid}/statm") as f:
        return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name")
    ap.add_argument("--step", type=int, default=250)
    ap.add_argument("--step-s", type=float, default=180)
    ap.add_argument("--period", type=float, default=20)
    ap.add_argument("--max-n", type=int, default=2000)
    ap.add_argument("--limit-mb", type=float, default=3072)
    ap.add_argument("--hold-s", type=float, default=180)
    a = ap.parse_args()
    d = os.path.join(WORK, "runs", a.name)
    os.makedirs(d, exist_ok=True)
    p = engine.start(a.name)
    time.sleep(2)
    samp = os.path.join(d, "samples.jsonl")
    sp = subprocess.Popen(["taskset", "-c", OTHER_CPUS, PY, os.path.join(HERE, "sampler.py"), str(p.pid), samp, "10"])
    steps = (a.max_n + a.step - 1) // a.step
    t0 = time.time() + 30
    t_end_max = t0 + steps * a.step_s + a.hold_s
    loads, log = [], open(os.path.join(d, "ramp.log"), "w")
    stopped_at = None
    for k in range(steps):
        ts = t0 + k * a.step_s
        # start the clients of this step (they connect and authenticate before ts)
        lp = subprocess.Popen(["taskset", "-c", OTHER_CPUS, PY, os.path.join(HERE, "load.py"), "period", str(a.step), str(t_end_max - ts),
                               os.path.join(d, f"lat.step{k}"), "--period", str(a.period), "--procs", "1", "--offset", str(k * a.step),
                               "--t-start", str(ts), "--stop-file", os.path.join(d, "STOP")], stdout=log, stderr=log)
        loads.append(lp)
        while time.time() < ts + a.step_s:
            if p.poll() is not None:
                break
            r = rss_mb(p.pid)
            if r > a.limit_mb and stopped_at is None:
                stopped_at = (k, r, time.time() - t0)
            time.sleep(2)
        log.write(f"step {k} done at {time.time() - t0:.0f}s: dbs {(k + 1) * a.step}, rss {rss_mb(p.pid) if p.poll() is None else -1:.0f} MB\n")
        log.flush()
        if p.poll() is not None or stopped_at:
            break
    if p.poll() is None and not stopped_at:
        time.sleep(max(0, t_end_max - time.time()))
    open(os.path.join(d, "STOP"), "w").close()   # clients stop at their next request and write their rows
    for lp in loads:
        try:
            lp.wait(a.period * 2 + 60)
        except subprocess.TimeoutExpired:
            lp.kill()
    with open(os.path.join(d, "meta.txt"), "w") as f:
        f.write(f"name={a.name}\nmode=ramp\nn={len(loads) * a.step}\nduration={time.time() - t0:.0f}\nextra={' '.join(sys.argv[2:])}\n"
                f"T_START {t0:.3f}\nengine_cmd={' '.join(engine.cmdline())}\nstopped_at={stopped_at}\n")
    time.sleep(10)
    open(samp + ".stop", "w").close()
    sp.wait(30)
    rc = engine.stop(p, 600) if p.poll() is None else p.returncode
    with open(os.path.join(d, "meta.txt"), "a") as f:
        f.write(f"engine_exit={rc}\n")
    print(a.name, "done; stopped_at", stopped_at, "engine rc", rc)


if __name__ == "__main__":
    main()
