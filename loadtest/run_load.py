#!/usr/bin/env python3
"""Orchestrates the load-test scenarios against mongo-server processes backed by MinIO.

    run_load.py machine
    run_load.py load  [--tenants 50000 --parallel 64]
    run_load.py s1 | s2 | s3 | s4   (sizes and durations are parameters, see --help)

Every scenario starts fresh server processes (so memory starts from a clean baseline), runs the `loadgen`
binary, and leaves its raw output in loadtest/out/<scenario>/: summary.json (latency, errors, verification),
samples.jsonl (CPU, memory, open tenants, object-store requests every 5 s), server-<port>.log, events.jsonl.
"""
import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release")
OUT = os.path.join(ROOT, "loadtest", "out")
# MinIO reached on its container address: through the published port every request also crosses docker-proxy,
# a userland proxy that used a noticeable share of the CPU in the first run.
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://172.18.0.2:9000")


def now_ms():
    return int(time.time() * 1000)


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def minio_pid():
    out = sh("pgrep -x minio | head -1")
    return int(out) if out else None


def minio_usage(prefix="/data/bench/tenants"):
    """Disk blocks (KB), file bytes and number of objects under the tenants prefix, measured inside the MinIO container."""
    blocks = sh(f"docker exec minio sh -c 'du -sk {prefix} 2>/dev/null' | cut -f1")
    files = sh(f"docker exec minio sh -c \"find {prefix} -type f -printf '%s\\n' 2>/dev/null | awk '{{s+=\\$1;n++}} END {{print s\\\" \\\"n}}'\"")
    b, n = (files.split() + ["0", "0"])[:2]
    return {"disk_kb": int(blocks or 0), "file_bytes": int(float(b or 0)), "objects": int(float(n or 0))}


class Server:
    def __init__(self, port, holder, outdir, idle, ttl, extra=()):
        self.port, self.holder, self.idle, self.ttl, self.extra = port, holder, idle, ttl, list(extra)
        self.log = os.path.join(outdir, f"server-{port}.log")
        self.proc = None

    def start(self):
        import socket
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
            raise RuntimeError(f"port {self.port} is already in use (a stray server?)")
        except OSError:
            pass
        f = open(self.log, "ab")
        self.proc = subprocess.Popen(
            [os.path.join(BIN, "mongo-server"), "--listen", f"127.0.0.1:{self.port}", "--idle-secs", str(self.idle), "--holder", self.holder, "--lease-ttl-secs", str(self.ttl), "--endpoint", ENDPOINT] + self.extra,
            stdout=f, stderr=f, cwd=ROOT)
        for _ in range(100):
            try:
                import socket
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return self
            except OSError:
                time.sleep(0.1)
        raise RuntimeError(f"server {self.port} did not start")

    def kill(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def stop(self, timeout=300):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def machine(a):
    cpu = sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2").strip()
    info = {
        "cpus": os.cpu_count(), "cpu_model": cpu,
        "ram_mb": int(sh("free -m | awk '/Mem:/ {print $2}'")), "ram_free_mb": int(sh("free -m | awk '/Mem:/ {print $7}'")),
        "disk_total_gb": float(sh("df -BG --output=size / | tail -1 | tr -dc 0-9")), "disk_free_gb": float(sh("df -BG --output=avail / | tail -1 | tr -dc 0-9")),
        "kernel": sh("uname -r"), "open_file_limit": int(sh("ulimit -n")),
        "minio_pid": minio_pid(),
    }
    os.makedirs(OUT, exist_ok=True)
    json.dump(info, open(os.path.join(OUT, "machine.json"), "w"), indent=1)
    print(json.dumps(info, indent=1))


def loadgen_run(a, name, outdir, uris, extra, ports_servers=None):
    cmd = [os.path.join(BIN, "loadgen"), "run", "--name", name, "--uris", ",".join(uris), "--out", outdir,
           "--pool-size", str(a.pool_size), "--worker-threads", str(a.loadgen_threads), "--sample-secs", str(a.sample_secs)]
    mp = minio_pid()
    if mp:
        cmd += ["--proc", f"minio={mp}"]
    cmd += [str(x) for x in extra]
    print("+", " ".join(cmd), flush=True)
    return subprocess.Popen(cmd, stdout=open(os.path.join(outdir, "loadgen.stdout"), "w"), stderr=open(os.path.join(outdir, "loadgen.stderr"), "w"), cwd=ROOT)


def fresh_dir(name):
    d = os.path.join(OUT, name)
    os.makedirs(d, exist_ok=True)
    for f in os.listdir(d):
        os.remove(os.path.join(d, f))
    return d


def scenario_load(a):
    d = fresh_dir("load")
    srv = Server(28001, "L", d, a.idle_secs, a.lease_ttl).start()
    before = minio_usage()
    t0 = time.time()
    lg = subprocess.Popen([os.path.join(BIN, "loadgen"), "load", "--uris", "mongodb://127.0.0.1:28001", "--tenants", str(a.tenants), "--first", str(a.first), "--docs", str(a.docs),
                           "--doc-bytes", str(a.doc_bytes), "--parallel", str(a.parallel), "--worker-threads", str(a.loadgen_threads), "--out", os.path.join(d, "load.json")], cwd=ROOT)
    # Sample the server, MinIO and the loader every 5 s while the load runs.
    samples = open(os.path.join(d, "samples.jsonl"), "w")
    pids = {"server": srv.proc.pid, "minio": minio_pid(), "loadgen": lg.pid}
    last = {}
    while lg.poll() is None:
        time.sleep(5)
        line = {"t": time.time() - t0}
        for n, pid in pids.items():
            try:
                st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
                cpu = (int(st[11]) + int(st[12])) / 100.0
                rss = int(open(f"/proc/{pid}/statm").read().split()[1]) * 4096 / 1048576
                line[n] = {"cpu_s": cpu, "rss_mb": rss}
            except OSError:
                pass
        samples.write(json.dumps(line) + "\n")
        samples.flush()
    secs = time.time() - t0
    srv.stop()
    after = minio_usage()
    load = json.load(open(os.path.join(d, "load.json")))
    n = load["loaded"]
    res = {"load": load, "usage_before": before, "usage_after": after, "wall_seconds": secs,
           "per_tenant": {k: (after[k] - before[k]) / max(n, 1) for k in after}}
    json.dump(res, open(os.path.join(d, "result.json"), "w"), indent=1)
    print(json.dumps(res["per_tenant"], indent=1))


def scenario_common(a, name, ports, extra, idle=None, holder_prefix="S", ttl=None, verify=False, hooks=None):
    d = fresh_dir(name)
    servers = [Server(p, f"{holder_prefix}{i}", d, idle if idle is not None else a.idle_secs, ttl if ttl is not None else a.lease_ttl).start() for i, p in enumerate(ports)]
    uris = [f"mongodb://127.0.0.1:{p}" for p in ports]
    events = open(os.path.join(d, "events.jsonl"), "w")
    lg = loadgen_run(a, name, d, uris, extra + (["--verify", "true"] if verify else []))
    if hooks:
        hooks(servers, lg, events, d)
    rc = lg.wait()
    for s in servers:
        s.stop()
    events.close()
    print(f"{name}: loadgen exit {rc}")
    return d


def scenario_s1(a):
    scenario_common(a, "s1", [28001], ["--mode", "rotate", "--start", a.s1_start, "--pool", a.s1_pool, "--active", a.s1_active, "--lifetime-secs", a.lifetime_secs,
                                      "--rate", a.rate, "--duration-secs", a.s1_secs, "--docs", a.docs])


def scenario_s2(a):
    scenario_common(a, "s2", [28001], ["--mode", "burst", "--start", a.s2_start, "--pool", a.s2_tenants, "--active", a.s2_tenants, "--ramp-secs", a.s2_ramp,
                                      "--hold-secs", a.s2_hold, "--rate", a.rate, "--duration-secs", a.s2_ramp + a.s2_hold + 5, "--docs", a.docs])


def scenario_s3(a):
    scenario_common(a, "s3", [28001], ["--mode", "rotate", "--start", a.s1_start, "--pool", a.s1_pool, "--active", a.s1_active, "--lifetime-secs", a.lifetime_secs,
                                      "--rate", a.rate, "--duration-secs", a.s3_secs, "--docs", a.docs, "--hot-tenant", a.hot_tenant, "--hot-rate", a.hot_rate])


def scenario_s4(a):
    def hooks(servers, lg, events, d):
        rng = random.Random(4)
        kills = a.s4_kills
        t_start = time.time()
        gap = a.s4_secs / (kills + 1)
        for k in range(kills):
            target_t = t_start + gap * (k + 1) + rng.uniform(-gap / 6, gap / 6)
            while time.time() < target_t:
                if lg.poll() is not None:
                    return
                time.sleep(0.2)
            s = rng.choice(servers)
            t0 = now_ms()
            s.kill()
            t1 = now_ms()
            events.write(json.dumps({"event": "kill", "holder": s.holder, "port": s.port, "unix_ms_before": t0, "unix_ms_after": t1}) + "\n")
            events.flush()
            time.sleep(rng.uniform(0.5, 2.0))
            t2 = now_ms()
            s.start()
            events.write(json.dumps({"event": "restart", "holder": s.holder, "port": s.port, "unix_ms_before": t2, "unix_ms_after": now_ms()}) + "\n")
            events.flush()
    scenario_common(a, "s4", [28001, 28002], ["--mode", "rotate", "--start", a.s4_start, "--pool", a.s4_pool, "--active", a.s4_active, "--lifetime-secs", a.lifetime_secs,
                                              "--rate", a.rate, "--duration-secs", a.s4_secs, "--docs", a.docs, "--max-attempts", 200, "--retry-backoff-ms", 100, "--op-timeout-secs", 15],
                    ttl=a.s4_lease_ttl, verify=True, hooks=hooks)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario", choices=["machine", "load", "s1", "s2", "s3", "s4"])
    ap.add_argument("--tenants", type=int, default=50000)
    ap.add_argument("--first", type=int, default=0)
    ap.add_argument("--docs", type=int, default=50)
    ap.add_argument("--doc-bytes", type=int, default=1000)
    ap.add_argument("--parallel", type=int, default=64)
    ap.add_argument("--idle-secs", type=int, default=30, help="server closes a tenant after this many idle seconds")
    ap.add_argument("--lease-ttl", type=int, default=30)
    ap.add_argument("--pool-size", type=int, default=256)
    ap.add_argument("--loadgen-threads", type=int, default=2)
    ap.add_argument("--sample-secs", type=int, default=5)
    ap.add_argument("--rate", type=float, default=1.0)
    ap.add_argument("--lifetime-secs", type=int, default=60)
    ap.add_argument("--s1-active", type=int, default=1000)
    ap.add_argument("--s1-secs", type=int, default=600)
    ap.add_argument("--s1-start", type=int, default=0)
    ap.add_argument("--s1-pool", type=int, default=20000)
    ap.add_argument("--s2-tenants", type=int, default=5000)
    ap.add_argument("--s2-start", type=int, default=30000)
    ap.add_argument("--s2-ramp", type=int, default=60)
    ap.add_argument("--s2-hold", type=int, default=120)
    ap.add_argument("--s3-secs", type=int, default=600)
    ap.add_argument("--hot-tenant", type=int, default=49999)
    ap.add_argument("--hot-rate", type=float, default=500.0)
    ap.add_argument("--s4-active", type=int, default=1000)
    ap.add_argument("--s4-secs", type=int, default=300)
    ap.add_argument("--s4-kills", type=int, default=10)
    ap.add_argument("--s4-start", type=int, default=20000)
    ap.add_argument("--s4-pool", type=int, default=10000)
    ap.add_argument("--s4-lease-ttl", type=int, default=10)
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    {"machine": machine, "load": scenario_load, "s1": scenario_s1, "s2": scenario_s2, "s3": scenario_s3, "s4": scenario_s4}[a.scenario](a)


if __name__ == "__main__":
    main()
