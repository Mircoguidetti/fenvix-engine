"""One measured run: fresh engine (all databases asleep in MinIO), sampler every 10 s, load clients, then the engine is
stopped. Raw files in WORK/runs/<name>/; the analysis copies the CSVs into results-density/.
usage: run_scenario.py NAME MODE N DURATION_S [load.py options...]"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def main():
    name, mode, n, dur = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
    extra = sys.argv[5:]
    d = os.path.join(WORK, "runs", name)
    os.makedirs(d, exist_ok=True)
    p = engine.start(name)
    time.sleep(2)
    samp = os.path.join(d, "samples.jsonl")
    sp = subprocess.Popen(["taskset", "-c", OTHER_CPUS, PY, os.path.join(HERE, "sampler.py"), str(p.pid), samp, "10"])
    lp = subprocess.run(["taskset", "-c", OTHER_CPUS, PY, os.path.join(HERE, "load.py"), mode, str(n), str(dur), os.path.join(d, "lat")] + extra,
                        capture_output=True, text=True)
    with open(os.path.join(d, "load.out"), "w") as f:
        f.write(lp.stdout + lp.stderr)
    t_start = [l for l in lp.stdout.splitlines() if l.startswith("T_START")]
    with open(os.path.join(d, "meta.txt"), "w") as f:
        f.write(f"name={name}\nmode={mode}\nn={n}\nduration={dur}\nextra={' '.join(extra)}\n{t_start[0] if t_start else 'T_START ?'}\n"
                f"engine_cmd={' '.join(engine.cmdline())}\n")
    time.sleep(10)  # one more sample after the load
    open(samp + ".stop", "w").close()
    sp.wait(30)
    rc = engine.stop(p)
    with open(os.path.join(d, "meta.txt"), "a") as f:
        f.write(f"engine_exit={rc}\n")
    print(name, "done; load rc", lp.returncode, "engine rc", rc)


if __name__ == "__main__":
    main()
