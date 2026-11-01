#!/usr/bin/env python3
"""Read benchmark of the engine against mongod 8.0, 1 core (bench/perf, see docs/perf-letture.md).

Differences from bench/mongod/run_matrix.py:
  - only 1 core for the database (CPU 0), only the read workloads: `read` (find by _id) and `rw`
    (50% find by _id, 50% $set by _id); layouts 1 (1 db x 200 000 docs) and 100 (100 dbs x 2 000 docs);
  - the engine is loaded ONCE per layout, stopped, and its store directory kept as a snapshot. Every run starts a fresh
    engine on a copy of the snapshot: the runs of every build start from the same data, and no run needs a bulk load
    (the engine's bulk load can stall, see docs/bench-mongod.md; the stall is not part of this work);
  - mongod is loaded once per layout (fresh container) and both workloads run on it, `read` first.

Usage:
  bench.py snapshot LAYOUT                      load the engine once, keep the store under $BENCH_DATA/snap-LAYOUT
  bench.py run SERVER LAYOUT WORKLOAD CLIENTS [--warmup S] [--duration S] [--reps N] [--label TEXT] [--bin PATH]
                                                SERVER: fenvix | mongod. Appends to $BENCH_RESULTS/raw.jsonl.
  bench.py sweep SERVER LAYOUT WORKLOAD         short runs over 1, 2, 4 ... 32 clients
"""
import argparse, json, os, shutil, signal, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.environ.get("BENCH_BIN", os.path.join(HERE, ".bin"))
DATA = os.environ.get("BENCH_DATA", os.path.expanduser("~/benchdata"))
RES = os.environ.get("BENCH_RESULTS", os.path.join(HERE, "results"))
RAW = os.path.join(RES, "raw.jsonl")
LAYOUTS = {1: (1, 200_000), 100: (100, 2_000)}
PORT = 27017
DB_CPUS, GEN_CPUS = "0", "1-3"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def port_free():
    import socket
    s = socket.socket()
    try:
        s.connect(("127.0.0.1", PORT))
        return False
    except OSError:
        return True
    finally:
        s.close()


_engine = None  # the engine started by this script (waited for, so it leaves no zombie that pgrep would still see)


def stop_all():
    # The engine's clean shutdown can hang (docs/bench-mongod.md): SIGTERM, then SIGKILL after 30 s.
    global _engine
    if _engine is not None:
        _engine.terminate()
        try:
            _engine.wait(30)
        except subprocess.TimeoutExpired:
            log("engine did not stop in 30 s: SIGKILL")
            _engine.kill()
            _engine.wait()
        _engine = None
    # any other engine process (whatever its binary is called: mongo-server-c1 ... are cut to 15 characters in comm)
    def engines():
        out = subprocess.run(["ps", "-eo", "pid,comm"], capture_output=True, text=True).stdout.split("\n")[1:]
        return [int(l.split()[0]) for l in out if l.split()[1:2] and l.split()[1].startswith("mongo-server")]
    for p in engines():
        os.kill(p, signal.SIGTERM)
    for _ in range(30):
        if not engines():
            break
        time.sleep(1)
    else:
        log("engine did not stop in 30 s: SIGKILL")
        for p in engines():
            os.kill(p, signal.SIGKILL)
    subprocess.run(["docker", "rm", "-f", "bench-mongod"], capture_output=True)
    for _ in range(120):
        if port_free():
            return
        time.sleep(1)
    raise RuntimeError("port still busy")


def start_fenvix(store, binary):
    stop_all()
    logf = open(os.path.join(DATA, "fenvix-server.log"), "w")
    p = subprocess.Popen(["taskset", "-c", DB_CPUS, binary, "--listen", f"127.0.0.1:{PORT}", "--store", "local",
                          "--local-dir", store], stdout=logf, stderr=subprocess.STDOUT)
    global _engine
    _engine = p
    for _ in range(120):
        with open(os.path.join(DATA, "fenvix-server.log")) as f:
            if "listening on" in f.read():
                return p.pid
        time.sleep(0.5)
    raise RuntimeError("engine did not start")


def start_mongod():
    stop_all()
    d = os.path.join(DATA, "mongod")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    subprocess.run(["docker", "run", "-d", "--name", "bench-mongod", "--network", "host", "--cpuset-cpus", DB_CPUS,
                    "-v", f"{d}:/data/db", "mongo:8.0", "mongod", "--bind_ip", "127.0.0.1", "--port", str(PORT)],
                   check=True, capture_output=True)
    for _ in range(60):
        if subprocess.run(["docker", "exec", "bench-mongod", "mongosh", "--quiet", "--port", str(PORT), "--eval",
                           "db.runCommand({ping:1}).ok"], capture_output=True).returncode == 0:
            break
        time.sleep(1)
    return int(subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", "bench-mongod"], capture_output=True,
                              text=True, check=True).stdout.strip())


def loadgen(args):
    env = dict(os.environ, GOGC="400")
    out = subprocess.run(["taskset", "-c", GEN_CPUS, os.path.join(BIN, "loadgen")] + args, capture_output=True,
                         text=True, env=env, timeout=1800)
    if out.stderr.strip():
        log("loadgen stderr:", out.stderr.strip()[-600:])
    if out.returncode != 0:
        raise RuntimeError(f"loadgen failed: {out.stderr[-1500:]}")
    return json.loads(out.stdout.strip().splitlines()[-1])


def load(layout, clients):
    dbs, docs = LAYOUTS[layout]
    return loadgen(["-mode", "load", "-dbs", str(dbs), "-docs", str(docs), "-clients", str(clients)])


def paced_load(layout):
    """The engine's bulk load stalls for good after about 100 000 documents into one database (docs/bench-mongod.md, being
    fixed on fix-stall), even with one batch in flight. Loading in chunks of 10 000 documents with 12 s between chunks
    does not stall (MEASURED, 1 of 1 load): compaction is requested only by writes, at most once every 10 s."""
    dbs, docs = LAYOUTS[layout]
    chunk = 10_000 if docs > 10_000 else docs
    t0 = time.time()
    for lo in range(0, docs, chunk):
        loadgen(["-mode", "load", "-dbs", str(dbs), "-docs", str(min(lo + chunk, docs)), "-from", str(lo),
                 "-clients", "1"] + (["-no-drop"] if lo else []))
        if lo + chunk < docs:
            time.sleep(12)
    return time.time() - t0


def snapshot(layout, binary):
    snap = os.path.join(DATA, f"snap-{layout}")
    for attempt in range(1, 4):
        store = os.path.join(DATA, "fenvix-store")
        stop_all()
        shutil.rmtree(store, ignore_errors=True)
        os.makedirs(store)
        start_fenvix(store, binary)
        try:
            secs = paced_load(layout)
            log(f"loaded layout {layout} in {secs:.0f} s (attempt {attempt})")
            stop_all()
            shutil.rmtree(snap, ignore_errors=True)
            shutil.copytree(store, snap, symlinks=True)
            return
        except RuntimeError as e:
            log(f"load attempt {attempt} failed: {str(e)[-200:]}")
            stop_all()
    raise RuntimeError("load failed 3 times")


_mongod_layout = None


def fresh(server, layout, binary):
    """Starts a server ready for a run: the engine on a copy of the snapshot; mongod is loaded once per layout."""
    global _mongod_layout
    if server == "fenvix":
        stop_all()  # before the copy: the previous engine still writes into the store directory until it stops
        store = os.path.join(DATA, "fenvix-store")
        shutil.rmtree(store)
        shutil.copytree(os.path.join(DATA, f"snap-{layout}"), store, symlinks=True)
        return start_fenvix(store, binary)
    if _mongod_layout != layout:
        pid = start_mongod()
        r = load(layout, 16)
        log(f"mongod loaded layout {layout} in {r['seconds']:.0f} s")
        _mongod_layout = layout
        return pid
    return int(subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", "bench-mongod"], capture_output=True,
                              text=True, check=True).stdout.strip())


def steal_cpu0():
    """Jiffies of CPU 0 stolen by the hypervisor (the machine is shared: a noisy run shows up here)."""
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu0 "):
                return int(line.split()[8])
    return 0


def one(server, layout, workload, clients, warmup, duration, label, binary, rep):
    dbs, docs = LAYOUTS[layout]
    pid = fresh(server, layout, binary)
    st0 = steal_cpu0()
    r = loadgen(["-mode", "run", "-workload", workload, "-dbs", str(dbs), "-docs", str(docs), "-clients",
                 str(clients), "-warmup", str(warmup), "-duration", str(duration), "-pid", str(pid)])
    r.update(server=server, layout=layout, label=label, rep=rep, warmup=warmup, ts=time.strftime("%Y-%m-%dT%H:%M:%S"),
             steal_cpu0_pct=(steal_cpu0() - st0) / (warmup + duration))
    os.makedirs(RES, exist_ok=True)
    with open(RAW, "a") as f:
        f.write(json.dumps(r) + "\n")
    log(f"{label:24} {server:6} L={layout:3} {workload:4} c={clients:2} ops/s={r['ops_per_s']:7.0f} "
        f"ops/cpu_s={r.get('ops_per_cpu_s', 0):7.0f} srv={r['server_cores_used']:.2f} gen={r['gen_cores_used']:.2f} "
        f"p50={r['p50_ms']:.2f} p99={r['p99_ms']:.2f} err={r['errors_total']} steal={r['steal_cpu0_pct']:.1f}%")
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd")
    ap.add_argument("rest", nargs="*")
    ap.add_argument("--warmup", type=int, default=20)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--label", default="")
    ap.add_argument("--bin", default=os.path.join(BIN, "mongo-server"))
    a = ap.parse_args()
    os.makedirs(DATA, exist_ok=True)
    try:
        if a.cmd == "snapshot":
            snapshot(int(a.rest[0]), a.bin)
        elif a.cmd == "run":
            server, layout, workload, clients = a.rest[0], int(a.rest[1]), a.rest[2], int(a.rest[3])
            for rep in range(1, a.reps + 1):
                one(server, layout, workload, clients, a.warmup, a.duration, a.label, a.bin, rep)
        elif a.cmd == "sweep":
            server, layout, workload = a.rest[0], int(a.rest[1]), a.rest[2]
            for c in [1, 2, 4, 8, 16, 32]:
                one(server, layout, workload, c, 10, 20, a.label or "sweep", a.bin, 0)
        else:
            ap.error("unknown command")
    finally:
        stop_all()


if __name__ == "__main__":
    sys.exit(main())
