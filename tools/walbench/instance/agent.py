#!/usr/bin/env python3
"""walbench instance agent (Python standard library only; runs as root from user-data).

Roles (cfg.json, written by the orchestrator into user-data):
  client   the benchmark client (same AZ as the Express directory bucket and the EBS volume). Runs the plan, uploads
           every result to s3://<bucket>/runs/<run>/results/ as soon as it exists.
  node     a quorum log node (node0 in the client's AZ, node1 and node2 in two other AZs)
  standby  the second instance for the EBS reattach test (client's AZ)
Coordination with the orchestrator is through small objects under runs/<run>/markers/. Request counters of every
walbench process are summed every 30 s into runs/<run>/progress/<role>.json for the live spend log.
No credentials here: the instance profile gives them to walbench through the instance metadata service."""
import glob
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

HOME = os.environ.get("WALBENCH_HOME", "/opt/walbench")
LOCAL = os.environ.get("WALBENCH_LOCAL_TEST") == "1"  # local emulator test: no IMDS, no block device
CFG = json.load(open(f"{HOME}/cfg.json"))
PLAN = json.load(open(f"{HOME}/plan.json"))
BIN = f"{HOME}/walbench"
RUN, BUCKET, ROLE = CFG["run"], CFG["bucket"], CFG["role"]
ROOT = f"runs/{RUN}/"
TAG = f"{ROLE}{CFG.get('node_index', '')}"  # client | node0..2 | standby
VAR = os.environ.get("WALBENCH_VAR", "/var/walbench")
LIVE = f"{VAR}/live-{ROLE}{CFG.get('node_index', '')}"
WORK = f"{VAR}/work-{ROLE}{CFG.get('node_index', '')}"
os.makedirs(LIVE, exist_ok=True)
os.makedirs(WORK, exist_ok=True)
ENV = dict(os.environ, WALBENCH_LIVE_DIR=LIVE, AWS_REGION=CFG["region"], AWS_DEFAULT_REGION=CFG["region"])
LOGF = open(f"{WORK}/agent.log", "a")


def log(msg):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + msg
    print(line, flush=True)
    LOGF.write(line + "\n")
    LOGF.flush()


PUT_FAILS = [0]  # consecutive failed uploads: the run bucket is gone (teardown) or unreachable


def _run_ok(args, timeout):
    """True if the command succeeded; a timeout or any other error is just False (never raises)."""
    try:
        return subprocess.run(args, env=ENV, capture_output=True, timeout=timeout).returncode == 0
    except Exception:
        return False


def s3put(key, path):
    ok = _run_ok([BIN, "s3put", "--bucket", BUCKET, "--key", ROOT + key, "--file", path], 120)
    PUT_FAILS[0] = 0 if ok else PUT_FAILS[0] + 1
    return ok


def s3get(key, path):
    return _run_ok([BIN, "s3get", "--bucket", BUCKET, "--key", ROOT + key, "--file", path], 120)


def put_json(key, obj):
    try:
        p = f"{WORK}/.put-{threading.get_ident()}.json"
        json.dump(obj, open(p, "w"))
        return s3put(key, p)
    except Exception as e:
        log(f"put_json {key}: {type(e).__name__}")
        return False


def marker(name, obj=None):
    put_json(f"markers/{name}.json", dict(obj or {}, utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), role=ROLE))
    log(f"marker {name}")


def get_marker(name):
    p = f"{WORK}/.m-{name}.json"
    if s3get(f"markers/{name}.json", p):
        try:
            return json.load(open(p))
        except Exception:
            return {}
    return None


def wait_marker(name, timeout, every=5):
    t = time.time()
    while time.time() - t < timeout:
        m = get_marker(name)
        if m is not None:
            return m
        time.sleep(every)
    return None


def stopped():
    """Stop marker written by the orchestrator, or 3 failed uploads in a row (the bucket was deleted: the run is over)."""
    return PUT_FAILS[0] >= 3 or get_marker("stop") is not None


def imds(path):
    if LOCAL:
        return {"local-ipv4": "127.0.0.1"}.get(path, "local")
    tok = urllib.request.urlopen(urllib.request.Request("http://169.254.169.254/latest/api/token", method="PUT",
                                                        headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"}), timeout=5).read().decode()
    return urllib.request.urlopen(urllib.request.Request(f"http://169.254.169.254/latest/meta-data/{path}",
                                                         headers={"X-aws-ec2-metadata-token": tok}), timeout=5).read().decode()


def progress_loop():
    """Sum of the counters of every walbench process on this instance (live files: running and finished ones)."""
    while True:
        try:
            _progress_once()
        except Exception as e:  # the progress thread must never die
            log(f"progress: {type(e).__name__}")
        time.sleep(30)


def _progress_once():
    tot = {}
    for f in glob.glob(f"{LIVE}/*.json"):
        try:
            c = json.load(open(f))["counters"]
        except Exception:
            continue
        for k, v in c.items():
            if isinstance(v, list):
                cur = tot.get(k, [0] * len(v))
                tot[k] = [a + b for a, b in zip(cur, v)]
            else:
                tot[k] = tot.get(k, 0) + v
    put_json(f"progress/{TAG}.json", {"unix": time.time(), "counters": tot, "processes": len(glob.glob(f"{LIVE}/*.json")),
                                      "steps": sorted(CURRENT)})
    s3put(f"logs/{TAG}-agent.log", f"{WORK}/agent.log")


# -------------------------------------------------------------------------------------------------------- client
SKIP = set()  # backends that failed their smoke test
CURRENT = set()  # steps running now (reported in the progress file for the live spend line)
RESULTS = []


def step(name, args, timeout, backend=None):
    """Runs one walbench command writing results/<name>.json; uploads it; never raises."""
    if backend in SKIP:
        log(f"step {name}: skipped (backend {backend} failed its smoke test)")
        return None
    if stopped():
        log(f"step {name}: skipped (stop marker)")
        return None
    out = f"{WORK}/{name}.json"
    t = time.time()
    log(f"step {name}: start")
    # own process group, so a timeout or a stop kills the step and every child it started (crash-test writers)
    errf, outf = open(f"{WORK}/{name}.stderr", "w"), open(f"{WORK}/{name}.stdout", "w")
    proc = subprocess.Popen([BIN] + args + ["--out", out], env=ENV, stdout=outf, stderr=errf, start_new_session=True)
    rc = None
    CURRENT.add(name)
    last_check = time.time()
    while rc is None:
        try:
            rc = proc.wait(timeout=2)
            break
        except subprocess.TimeoutExpired:
            pass
        if time.time() - t > timeout:
            rc = "timeout"
        elif time.time() - last_check > 10:
            last_check = time.time()
            if stopped():
                rc = "stopped (stop marker)"
        if rc is not None:
            try:
                os.killpg(proc.pid, 9)
            except ProcessLookupError:
                pass
            proc.wait()
    CURRENT.discard(name)
    errf.close()
    outf.close()
    err, so = open(f"{WORK}/{name}.stderr").read()[-3000:], open(f"{WORK}/{name}.stdout").read()[-1000:]
    res = None
    if os.path.exists(out):
        try:
            res = json.load(open(out))
        except Exception:
            res = None
    if res is None:
        res = {"name": name, "error": f"exit {rc}", "stderr_tail": err, "stdout_tail": so}
    res["step_wall_s"] = time.time() - t
    res["step_exit"] = rc
    json.dump(res, open(out, "w"), indent=1)
    s3put(f"results/{name}.json", out)
    log(f"step {name}: exit {rc} in {time.time() - t:.0f} s; {so.strip()[-300:]}")
    RESULTS.append(name)
    return res


def bflags(backend):
    if backend == "s3":
        return ["--backend", "s3", "--bucket", BUCKET, "--prefix", f"{ROOT}wal/"]
    if backend == "express":
        return ["--backend", "express", "--bucket", CFG["express_bucket"], "--prefix", "wal/"]
    if backend == "ddb":
        return ["--backend", "ddb", "--table", CFG["table"]]
    if backend == "file":
        return ["--backend", "file", "--dir", f"{VAR}/wal/logs" if LOCAL else "/mnt/wal/logs"]
    if backend == "quorum":
        return ["--backend", "quorum", "--nodes", ",".join(CFG["nodes"])]
    raise ValueError(backend)


def cells(backend, variants):
    c = PLAN["cell"]
    for v in variants:
        for size in PLAN["sizes"]:
            for conc in PLAN["concs"]:
                name = f"cell-{backend}-{v}-{size}b-c{conc}"
                step(name, ["cell"] + bflags(backend) + ["--variant", v, "--size", str(size), "--conc", str(conc), "--writes", str(c["writes"]),
                                                          "--warmup", str(c["warmup"]), "--max-secs", str(c["max_secs"]), "--log", name, "--name", name],
                     timeout=c["max_secs"] + 300, backend=backend)


def smoke(backend, variant):
    name = f"smoke-{backend}"
    r = step(name, ["cell"] + bflags(backend) + ["--variant", variant, "--size", "1024", "--conc", "2", "--writes", "20", "--warmup", "2",
                                                 "--max-secs", "30", "--log", name, "--name", name], timeout=180)
    if not r or r.get("writes_acked", 0) < 20 or r.get("write_errors", 1) > 0:
        SKIP.add(backend)
        log(f"SMOKE FAILED for {backend}: it is skipped for the rest of the run")


def prepare_volume():
    if LOCAL:
        os.makedirs(f"{VAR}/wal/logs", exist_ok=True)
        return
    dev = "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_" + CFG["volume"].replace("-", "")
    for _ in range(600):
        if os.path.exists(dev):
            break
        time.sleep(1)
    else:
        log("log volume never appeared")
        SKIP.add("file")
        return
    subprocess.run(["mkfs.ext4", "-F", "-q", dev], check=True)
    os.makedirs("/mnt/wal", exist_ok=True)
    subprocess.run(["mount", "-o", "noatime", dev, "/mnt/wal"], check=True)
    os.makedirs("/mnt/wal/logs", exist_ok=True)
    info = subprocess.run(["lsblk", "-o", "NAME,SIZE,MODEL,SERIAL,MOUNTPOINT", "-J"], capture_output=True, text=True).stdout
    put_json("results/env-volume.json", {"device": dev, "lsblk": info, "fs": "ext4 (mkfs defaults), mount -o noatime"})
    log("log volume formatted and mounted")


class _Group:
    def __init__(self, p):
        self.p = p

    def kill_group(self):
        try:
            os.killpg(self.p.pid, 9)
        except ProcessLookupError:
            pass


LOCAL_NODE = None


def start_local_node():
    """Quota allows only 3 instances: quorum node 0 runs on the client (same AZ as node 0 would be), log on its root disk."""
    global LOCAL_NODE
    port = CFG.get("node_port", 7700)
    p = subprocess.Popen(["bash", f"{HOME}/node-supervisor.sh", BIN, f"0.0.0.0:{port}", f"{VAR}/node0"], stdout=open(f"{WORK}/node0.log", "a"),
                         stderr=subprocess.STDOUT, start_new_session=True)
    LOCAL_NODE = _Group(p)
    time.sleep(2)
    marker("node-0", {"ip": imds("local-ipv4"), "port": port, "az_id": imds("placement/availability-zone-id"), "on_client": True})


def client():
    env = {"instance_type": imds("instance-type"), "az": imds("placement/availability-zone"), "az_id": imds("placement/availability-zone-id"),
           "kernel": os.uname().release, "nproc": os.cpu_count()}
    put_json("results/env-client.json", env)
    prepare_volume()
    if CFG.get("node0_on_client"):
        start_local_node()
    nodes = []
    for i in range(3):
        m = wait_marker(f"node-{i}", 900)
        if m is None:
            log(f"node {i} never registered: quorum skipped")
            SKIP.add("quorum")
            break
        nodes.append(f"{m['ip']}:{m.get('port', 7700)}")
    CFG["nodes"] = nodes
    for b, v in [("s3", "perwrite"), ("express", "perwrite"), ("ddb", "perwrite"), ("file", "w0"), ("quorum", "w0")]:
        if b not in SKIP:
            smoke(b, v)
    marker("smoke-done", {"skipped": sorted(SKIP)})
    cr = PLAN["crash"]

    def crash_args(b, seed):
        extra = ["--prealloc-mb", "8"] if b in ("file", "quorum") else []
        return ["crashtest"] + bflags(b) + extra + ["--variants", cr["variants"][b], "--cycles", str(cr["cycles"]), "--kill-min-ms", str(cr["kill_min_ms"]),
                                                   "--kill-max-ms", str(cr["kill_max_ms"]), "--max-writes", str(cr["max_writes"]),
                                                   "--max-secs", str(cr["max_secs"]), "--log", f"cr{b}", "--seed", str(seed)]
    # loss evidence first, so an automatic stop later cannot cut it
    # ---- quorum (its separate nodes are terminated as soon as it is done)
    if "quorum" not in SKIP:
        step("quorumtest", ["quorumtest", "--nodes", ",".join(nodes), "--log", "qt", "--scale", str(PLAN["quorumtest"]["scale"])], timeout=600, backend="quorum")
        step("crash-quorum", crash_args("quorum", 5), timeout=cr["max_secs"] + 600, backend="quorum")
        cells("quorum", PLAN["serial_variants"])
    if LOCAL_NODE:
        LOCAL_NODE.kill_group()
    marker("quorum-done")
    # ---- crash tests of s3, express, ddb, file in parallel (latency is not measured here)
    ths = []
    for i, b in enumerate(PLAN["cloud_backends"] + ["file"]):
        th = threading.Thread(target=step, args=(f"crash-{b}", crash_args(b, 11 + i), cr["max_secs"] + 600, b))
        th.start()
        ths.append(th)
    for th in ths:
        th.join()
    # ---- EBS (local log on the gp3 volume)
    cells("file", PLAN["serial_variants"])
    marker("standby-needed")
    # ---- cloud backends
    for b in PLAN["cloud_backends"]:
        cells(b, PLAN["cloud_variants"])
    f = PLAN["fleet"]
    for b in PLAN["cloud_backends"]:
        for layout, v in f["cells"]:
            name = f"fleet-{b}-{layout}-{v}"
            step(name, ["fleet"] + bflags(b) + ["--layout", layout, "--variant", v, "--dbs", str(f["dbs"]), "--rate", str(f["rate"]), "--secs", str(f["secs"]),
                                                "--warm-secs", str(f["warm_secs"]), "--size", str(f["size"]), "--log", name, "--name", name],
                 timeout=f["secs"] + 600, backend=b)
    # ---- EBS reattach: this instance is holder A, the standby is holder B; the orchestrator moves the volume
    if "file" not in SKIP and not stopped() and not LOCAL:
        subprocess.run(["umount", "/mnt/wal"])
        h = subprocess.Popen([BIN, "ebs-holder", "--vol", CFG["volume"], "--me", "A", "--bucket", BUCKET, "--coord", f"{ROOT}ebs/"],
                             env=ENV, stdout=open(f"{WORK}/holder.log", "a"), stderr=subprocess.STDOUT)
        marker("reattach-ready")
        wait_marker("reattach-done", PLAN["reattach"]["max_minutes"] * 60, every=10)
        h.kill()
        s3put("logs/client-holder.log", f"{WORK}/holder.log")
    marker("client-done", {"results": RESULTS, "skipped": sorted(SKIP)})


def node():
    i = CFG["node_index"]
    ip, az = imds("local-ipv4"), imds("placement/availability-zone-id")
    port = CFG.get("node_port", 7700)
    subprocess.Popen(["bash", f"{HOME}/node-supervisor.sh", BIN, f"0.0.0.0:{port}", f"{VAR}/node{i}"], stdout=open(f"{WORK}/node.log", "a"), stderr=subprocess.STDOUT)
    time.sleep(2)
    marker(f"node-{i}", {"ip": ip, "port": port, "az_id": az})
    while True:
        time.sleep(60)
        s3put(f"logs/node{i}.log", f"{WORK}/node.log")


def standby():
    put_json("results/env-standby.json", {"instance_type": imds("instance-type"), "az_id": imds("placement/availability-zone-id")})
    subprocess.run([BIN, "ebs-holder", "--vol", CFG["volume"], "--me", "B", "--bucket", BUCKET, "--coord", f"{ROOT}ebs/"],
                   env=ENV, stdout=open(f"{WORK}/holder.log", "a"), stderr=subprocess.STDOUT)


if __name__ == "__main__":
    threading.Thread(target=progress_loop, daemon=True).start()
    log(f"agent start: role {ROLE}")
    try:
        {"client": client, "node": node, "standby": standby}[ROLE]()
    except Exception as e:
        log(f"agent error: {type(e).__name__}: {e}")
        marker(f"error-{ROLE}{CFG.get('node_index', '')}", {"error": f"{type(e).__name__}: {str(e)[:500]}"})
        if ROLE == "client":
            marker("client-done", {"error": True, "results": RESULTS})
    time.sleep(40)  # one more progress upload
