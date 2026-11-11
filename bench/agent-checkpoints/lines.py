"""Line runner of the local equal-durability benchmark (P-bench-local, 2026-11-11): one line of equal-durability.json at a
time, in Docker on one network, with bench.py run from the host against it.

  lines.py run LINE --out DIR [--workers N] [--profile standard] [--read-sample 300] [--erase-users 100]
      down everything of ours -> disk check -> up LINE -> durability checks -> bench.py (with the cold phase calling
      "lines.py restart LINE") -> settings, logs, CPU and memory into DIR -> down (containers and data removed)
  lines.py up LINE | down | restart LINE | check LINE | hosts

Rules it applies (P-bench-local section 2):
  - every database member, of every system, runs with the same limits: --cpus 1 --memory 3g (LIMITS below);
  - MinIO (plays S3 for our two lines) has its own container and is NOT capped, the same for both of our lines;
  - one Docker network (NET, fixed addresses so the log copies can name their peers); the host resolves the container
    names through a block in /etc/hosts, so bench.py uses the connection strings of equal-durability.json unchanged;
  - durability exactly as equal-durability.md, checked BEFORE the run (check_line): Postgres pg_stat_replication shows
    both standbys streaming with sync_state quorum; mongod rs.status() has 3 healthy members; ours: the cook answers
    and serverStatus().fenvix.quaderno exists (bench.py records quaderno.sent before and after the write phase, and
    this runner fails the line if it did not grow);
  - the password comes from BENCH_PW (environment only; passed to containers as `-e NAME` without a value, so it never
    appears on a command line or in a file). Postgres standbys replicate over a trust rule for replication
    connections from the bench network only, so no password is written into a standby's primary_conninfo.
  - images: postgres:16, mongo:7.0, fenvix-step9-machine (step9/local/Dockerfile) for our log copies, cook, spare and
    MinIO (the MinIO server binary of judge/setup.sh, mounted read-only).
Our binaries are read from this checkout: target/release/mongo-server and proto/quaderno/target/release/pq (copied
there from the engine worktree; their sha256 goes into DIR/settings.json).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
CONF = json.load(open(os.path.join(HERE, "equal-durability.json")))
LINES = {l["name"]: l for l in CONF["lines"]}
NET, SUBNET = "fxbench", "172.31.77.0/24"
IP = {"minio": "172.31.77.10",
      "pg-primary": "172.31.77.20", "pg-standby1": "172.31.77.21", "pg-standby2": "172.31.77.22", "pg-single": "172.31.77.23",
      "mongo-a": "172.31.77.30", "mongo-b": "172.31.77.31", "mongo-c": "172.31.77.32", "mongo-single": "172.31.77.33",
      "log-0": "172.31.77.40", "log-1": "172.31.77.41", "log-2": "172.31.77.42", "cook": "172.31.77.43", "spare": "172.31.77.44",
      "cook-nolog": "172.31.77.45"}
LIMITS = ["--cpus", "1", "--memory", "3g"]
LABEL = ["--label", "fxbench=1"]
MACHINE_IMAGE = "fenvix-step9-machine"
SRV = os.path.join(ROOT, "target/release/mongo-server")
PQ = os.path.join(ROOT, "proto/quaderno/target/release/pq")
MINIO_BIN = os.environ.get("MINIO_BIN", os.path.expanduser("~/.fenvix-judge/env/bin/minio"))
PYLIB = os.environ.get("BENCH_PYLIB", os.path.expanduser("~/.fenvix-agentbench-venv/lib/python3.12/site-packages"))
PY = os.environ.get("BENCH_PY", os.path.expanduser("~/.fenvix-agentbench-venv/bin/python"))
# MinIO plays S3 locally: these are MinIO's own local credentials (the same fixed values as step9/local/local-run.sh),
# never AWS credentials
S3 = {"endpoint": f"http://{IP['minio']}:9000", "bucket": "fxbench", "key": "minio-local", "secret": "minio-local-secret",
      "region": "eu-west-1"}
S3_ENV = {"AWS_ACCESS_KEY_ID": S3["key"], "AWS_SECRET_ACCESS_KEY": S3["secret"], "AWS_REGION": S3["region"],
          "AWS_DEFAULT_REGION": S3["region"], "AWS_ALLOW_HTTP": "true"}
RESTART_T = "120"  # seconds docker waits for a clean stop before SIGKILL, the same for every container
HOSTS_BEGIN, HOSTS_END = "# fxbench begin (bench/agent-checkpoints/lines.py)", "# fxbench end"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def sh(cmd: list[str], check=True, capture=True, env=None, timeout=None) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, capture_output=capture, text=True, env=env, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} -> {r.returncode}: {(r.stderr or '')[-2000:]}")
    return r


def pw() -> str:
    p = os.environ.get("BENCH_PW")
    if not p:
        sys.exit("BENCH_PW is not set")
    return p


def uri_of(line: str) -> str:
    return LINES[line]["uri"].replace("${BENCH_PW}", urllib.parse.quote(pw(), safe=""))


def members(line: str) -> list[str]:
    return list(LINES[line]["containers"])


def all_containers(line: str) -> list[str]:
    return members(line) + (["minio"] if LINES[line]["target"] == "fenvix" else [])


def wait_until(what: str, fn, timeout=300, every=1.0):
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        try:
            v = fn()
            if v:
                return v
        except Exception as e:  # noqa: BLE001 - retried until the timeout
            last = e
        time.sleep(every)
    raise RuntimeError(f"timeout waiting for {what} ({timeout} s): {last!r}")


# ----------------------------------------------------------------------------------------------------------------------
# network, hosts, down
# ----------------------------------------------------------------------------------------------------------------------

def hosts() -> None:
    """The host resolves the container names (the client runs on the host, the same way for every line)."""
    cur = open("/etc/hosts").read()
    if HOSTS_BEGIN in cur:
        cur = cur.split(HOSTS_BEGIN)[0].rstrip("\n") + "\n" + cur.split(HOSTS_END, 1)[1].lstrip("\n")
    block = "\n".join([HOSTS_BEGIN] + [f"{ip} {n}" for n, ip in IP.items()] + [HOSTS_END])
    open("/etc/hosts", "w").write(cur.rstrip("\n") + "\n" + block + "\n")


def network() -> None:
    if sh(["docker", "network", "inspect", NET], check=False).returncode != 0:
        sh(["docker", "network", "create", "--subnet", SUBNET, NET])


def down() -> None:
    ids = sh(["docker", "ps", "-aq", "--filter", "label=fxbench=1"]).stdout.split()
    if ids:
        sh(["docker", "rm", "-f", "-v"] + ids)
    vols = sh(["docker", "volume", "ls", "-q", "--filter", "label=fxbench=1"]).stdout.split()
    if vols:
        sh(["docker", "volume", "rm", "-f"] + vols, check=False)


def run_container(name: str, image: str, args: list[str], cmd: list[str], capped=True, secret_env=None, env=None,
                  limits: list[str] | None = None) -> None:
    # --init (tini as PID 1, for every container of every system): a process that is PID 1 and installs no SIGTERM
    # handler ignores SIGTERM (the log copies did: each restart waited the full RESTART_T, then SIGKILL)
    c = ["docker", "run", "-d", "--init", "--name", name, "--hostname", name, "--network", NET, "--ip", IP[name]] + LABEL
    c += (LIMITS if capped else []) if limits is None else limits
    for k in (secret_env or {}):   # value handed through docker's environment, never on the command line
        c += ["-e", k]
    for k, v in (env or {}).items():
        c += ["-e", f"{k}={v}"]
    sh(c + args + [image] + cmd, env=dict(os.environ) | (secret_env or {}))


# ----------------------------------------------------------------------------------------------------------------------
# PostgreSQL
# ----------------------------------------------------------------------------------------------------------------------

PG_HBA_INIT = f"""#!/bin/bash
# fxbench: standbys replicate over a trust rule for replication connections from the bench network only
echo "host replication all {SUBNET} trust" >> "$PGDATA/pg_hba.conf"
"""
STANDBY_SH = """set -e
D=/var/lib/postgresql/data
if [ ! -s "$D/PG_VERSION" ]; then
  mkdir -p "$D"; chown -R postgres "$D"; chmod 700 "$D"
  gosu postgres pg_basebackup -D "$D" -R -X stream -d "host=pg-primary port=5432 user=app_user application_name=$APP"
fi
exec gosu postgres postgres -D "$D" -c hot_standby=on -c shared_buffers=768MB
"""


def pg_conn(host: str, db="postgres"):
    import psycopg
    return psycopg.connect(host=host, port=5432, user="app_user", password=pw(), dbname=db, autocommit=True, connect_timeout=5)


def pg_ready(host: str, db="postgres") -> bool:
    with pg_conn(host, db) as c:
        return c.execute("SELECT 1").fetchone()[0] == 1


# bench-local review F8: shared_buffers 25% of the member's 3 GiB, as aws-fixes sets it on AWS (B10, pgbox.sh: 25% of
# RAM); the stock 128 MB left Postgres's data in the page cache of its 3 GiB cgroup only. Every Postgres member.
PG_MEM_FLAGS = ["-c", "shared_buffers=768MB"]


def up_pg(name: str, primary_flags: list[str]) -> None:
    work = os.path.join(os.environ.get("FXBENCH_WORK", "/tmp/fxbench"), "pg-init")
    os.makedirs(work, exist_ok=True)
    init = os.path.join(work, "10-replication-hba.sh")
    open(init, "w").write(PG_HBA_INIT)
    os.chmod(init, 0o755)
    run_container(name, "postgres:16", ["-v", f"{init}:/docker-entrypoint-initdb.d/10-replication-hba.sh:ro"],
                  primary_flags + PG_MEM_FLAGS, secret_env={"POSTGRES_PASSWORD": pw()}, env={"POSTGRES_USER": "app_user", "POSTGRES_DB": "app_demo"})
    # the image's first start runs initdb on a socket-only temporary server, then restarts on TCP: wait for TCP
    wait_until(f"{name} accepting TCP", lambda: pg_ready(IP[name], "app_demo"), 180)


def up_postgres_sync() -> None:
    pc = LINES["postgres-sync"]["primary_conf"]
    flags = ["-c", f"wal_level={pc['wal_level']}", "-c", f"synchronous_commit={pc['synchronous_commit']}",
             "-c", f"max_wal_senders={pc['max_wal_senders']}"]
    # synchronous_standby_names is set with ALTER SYSTEM once both standbys stream (if it were a start flag, the image's
    # init steps would wait forever for standbys that do not exist yet)
    up_pg("pg-primary", flags)
    for i in (1, 2):
        n = f"pg-standby{i}"
        run_container(n, "postgres:16", ["--entrypoint", "bash"], ["-c", STANDBY_SH], env={"APP": f"standby{i}"})
    wait_until("both standbys streaming", lambda: len(pg_replication()) == 2 and all(r["state"] == "streaming" for r in pg_replication()), 300)
    with pg_conn(IP["pg-primary"]) as c:
        c.execute(f"ALTER SYSTEM SET synchronous_standby_names = {pc['synchronous_standby_names'].strip(chr(34))}")
        c.execute("SELECT pg_reload_conf()")
    wait_until("sync_state quorum", lambda: all(r["sync_state"] == "quorum" for r in pg_replication()) and len(pg_replication()) == 2, 120)


def pg_replication() -> list[dict]:
    with pg_conn(IP["pg-primary"]) as c:
        cur = c.execute("SELECT application_name, state, sync_state, sync_priority FROM pg_stat_replication ORDER BY 1")
        return [dict(zip(("application_name", "state", "sync_state", "sync_priority"), r)) for r in cur.fetchall()]


def pg_settings(host: str) -> dict:
    names = ("server_version", "wal_level", "synchronous_commit", "synchronous_standby_names", "max_wal_senders",
             "shared_buffers", "effective_cache_size", "work_mem", "fsync", "full_page_writes", "wal_compression",
             "default_toast_compression", "max_connections", "data_checksums", "hot_standby")
    with pg_conn(host) as c:
        return {n: c.execute(f"SHOW {n}").fetchone()[0] for n in names}


# ----------------------------------------------------------------------------------------------------------------------
# mongod
# ----------------------------------------------------------------------------------------------------------------------

def mongo_client(host: str, **kw):
    from pymongo import MongoClient
    return MongoClient(host, 27017, directConnection=True, serverSelectionTimeoutMS=5000, **kw)


def up_mongo_rs3() -> None:
    for n in ("mongo-a", "mongo-b", "mongo-c"):
        run_container(n, "mongo:7.0", [], ["mongod", "--replSet", "rs0", "--bind_ip_all"])
    for n in ("mongo-a", "mongo-b", "mongo-c"):
        wait_until(f"{n} up", lambda n=n: mongo_client(n).admin.command("ping"), 120)
    mongo_client("mongo-a").admin.command("replSetInitiate", {"_id": "rs0", "members": [
        {"_id": i, "host": f"{n}:27017"} for i, n in enumerate(("mongo-a", "mongo-b", "mongo-c"))]})
    wait_until("rs0: 3 healthy members and a primary", mongo_rs_ok, 180)


def mongo_rs_ok() -> bool:
    for n in ("mongo-a", "mongo-b", "mongo-c"):
        try:
            st = mongo_client(n).admin.command("replSetGetStatus")
        except Exception:  # noqa: BLE001
            continue
        ms = st["members"]
        return (len(ms) == 3 and all(m["health"] == 1 for m in ms) and sum(m["stateStr"] == "PRIMARY" for m in ms) == 1
                and sum(m["stateStr"] == "SECONDARY" for m in ms) == 2)
    return False


def up_mongo_single() -> None:
    run_container("mongo-single", "mongo:7.0", [], ["mongod", "--bind_ip_all"])
    wait_until("mongo-single up", lambda: mongo_client("mongo-single").admin.command("ping"), 120)


def mongo_settings(host: str) -> dict:
    c = mongo_client(host)
    ss = c.admin.command("serverStatus")
    out = {"version": ss["version"], "cmdline": c.admin.command("getCmdLineOpts")["argv"],
           "wiredTiger_cache_max_bytes": ss["wiredTiger"]["cache"]["maximum bytes configured"],
           "storageEngine": ss["storageEngine"]["name"],
           "hostInfo_memLimitMB": c.admin.command("hostInfo")["system"].get("memLimitMB")}
    try:
        out["rs_conf"] = c.admin.command("replSetGetConfig")["config"]
        out["rs_status"] = [{k: m.get(k) for k in ("name", "stateStr", "health")} for m in c.admin.command("replSetGetStatus")["members"]]
        out["defaultRWConcern"] = c.admin.command("getDefaultRWConcern")
    except Exception as e:  # noqa: BLE001 - single node
        out["rs"] = f"none ({type(e).__name__})"
    return json.loads(json.dumps(out, default=str))


# ----------------------------------------------------------------------------------------------------------------------
# ours: MinIO, three log copies, cook, spare / cook-nolog
# ----------------------------------------------------------------------------------------------------------------------

def bin_sha() -> dict:
    return {p: hashlib.sha256(open(p, "rb").read()).hexdigest() for p in (SRV, PQ)}


def up_minio() -> None:
    run_container("minio", MACHINE_IMAGE, ["-v", f"{MINIO_BIN}:/opt/minio:ro"],
                  ["/opt/minio", "server", "/data", "--address", ":9000", "--console-address", ":9001"],
                  # bench-local review F8: MinIO's memory (and so its page cache, which held all of our data) is capped
                  # at 3 GiB like every database member; its CPU stays uncapped, as the prompt allows
                  limits=["--memory", "3g"],
                  env={"MINIO_ROOT_USER": S3["key"], "MINIO_ROOT_PASSWORD": S3["secret"],
                       # deleted objects go to MinIO's trash (.minio.sys/tmp) and are purged every 5 minutes by default;
                       # with our server's WAL and compaction churn that held 8 GB at once on this small disk (the
                       # rehearsal of 2026-11-11). S3 has no such trash. Purged every 10 s here, for both our lines.
                       "MINIO_API_DELETE_CLEANUP_INTERVAL": "10s",
                       # MinIO's own transparent compression of what it keeps on its disk (S2), for every object: the
                       # rehearsal of 2026-11-11 filled this sandbox's disk at 80% of our line's writes (more than 25 GB
                       # of uncompressed SlateDB SSTs, sealed runs and GC-pending objects). It changes nothing our
                       # server sends or reads: listings and HEAD give the original sizes (so the stored bytes measured
                       # are the same) and range reads return the same bytes (tested); it costs MinIO CPU (MinIO is
                       # not capped; its CPU is recorded in cpu_mem.jsonl). S3 bills the original size.
                       "MINIO_COMPRESSION_ENABLE": "on", "MINIO_COMPRESSION_EXTENSIONS": "*", "MINIO_COMPRESSION_MIME_TYPES": "*"})
    import boto3
    s3 = boto3.client("s3", endpoint_url=S3["endpoint"], region_name=S3["region"], aws_access_key_id=S3["key"],
                      aws_secret_access_key=S3["secret"])
    wait_until("MinIO up", lambda: s3.list_buckets() is not None, 60)
    s3.create_bucket(Bucket=S3["bucket"])


def server_cmd(name: str, log_on: bool) -> list[str]:
    cmd = ["/opt/fx/mongo-server", "--listen", "0.0.0.0:27017", "--store", "s3", "--bucket", S3["bucket"],
           "--region", S3["region"], "--endpoint", S3["endpoint"], "--replica-set", "rs0", "--advertise", f"{IP[name]}:27017",
           "--auth",
           # integration-3's product defaults need no flag (sealed runs 2, the compactor, derived documents for the
           # saver's collections `checkpoints` and `checkpoint_writes`, the one-wave reader, lz4) except this one: a
           # collection gets derived documents only when its customer key is thread_id (agentck.rs), and the server's
           # default customer key list is empty (the c2-delta-log server check passed the same flag)
           "--customer-key", "checkpoints=thread_id,checkpoint_writes=thread_id"]
    if log_on:
        cmd += ["--quaderno", ",".join(f"{IP[f'log-{i}']}:7200" for i in range(3)), "--quaderno-id", "cell1"]
    return cmd


def run_ours(name: str, cmd: list[str]) -> None:
    run_container(name, MACHINE_IMAGE, ["-v", f"{SRV}:/opt/fx/mongo-server:ro", "-v", f"{PQ}:/opt/fx/pq:ro",
                                        "-v", f"{PYLIB}:/opt/pylib:ro"], cmd, env=S3_ENV | {"PYTHONPATH": "/opt/pylib"})


def create_root(name: str) -> None:
    """The root user, created from inside the cook (the server's localhost exception), password from BENCH_PW."""
    code = ("import os,time\nfrom pymongo import MongoClient\nfrom pymongo.errors import OperationFailure\n"
            "for k in range(90):\n"
            "    try:\n"
            "        c=MongoClient('mongodb://127.0.0.1:27017/?directConnection=true',serverSelectionTimeoutMS=3000); c.admin.command('ping'); break\n"
            "    except Exception: time.sleep(1)\n"
            "try:\n    c.admin.command('createUser','root',pwd=os.environ['BENCH_PW'],roles=[{'role':'root','db':'admin'}]); print('root user created')\n"
            "except OperationFailure as e: print('root user not created:', e.code)\n")
    r = sh(["docker", "exec", "-e", "BENCH_PW", name, "python3", "-c", code], env=dict(os.environ), timeout=180)
    log(name, r.stdout.strip())


def ours_ready(line: str) -> bool:
    from pymongo import MongoClient
    c = MongoClient(uri_of(line), serverSelectionTimeoutMS=5000)
    c.admin.command("ping")
    return True


def up_fenvix(line: str) -> None:
    up_minio()
    log_on = line == "fenvix-log"
    if log_on:
        for i in range(3):
            peers = ",".join(f"{j}={IP[f'log-{j}']}:7100" for j in range(3) if j != i)
            run_ours(f"log-{i}", ["/opt/fx/pq", "replica", "--id", str(i), "--dir", "/data/log", "--peer-listen", "0.0.0.0:7100",
                                  "--client-listen", "0.0.0.0:7200", "--peers", peers])
        time.sleep(3)
        for i in range(3):
            st = sh(["docker", "inspect", "-f", "{{.State.Running}}", f"log-{i}"]).stdout.strip()
            if st != "true":
                raise RuntimeError(f"log-{i} did not start: {sh(['docker', 'logs', f'log-{i}'], check=False).stderr[-2000:]}")
        run_ours("cook", server_cmd("cook", True))
        create_root("cook")
        run_ours("spare", server_cmd("spare", True))
        wait_until("spare listening", lambda: "listening on" in docker_logs("spare"), 120)
    else:
        run_ours("cook-nolog", server_cmd("cook-nolog", False))
        create_root("cook-nolog")
    wait_until("our server answers as root", lambda: ours_ready(line), 120)


def docker_logs(name: str) -> str:
    r = sh(["docker", "logs", name], check=False)
    return (r.stdout or "") + (r.stderr or "")


# ----------------------------------------------------------------------------------------------------------------------
# up / check / restart
# ----------------------------------------------------------------------------------------------------------------------

def up(line: str) -> None:
    network()
    hosts()
    t = LINES[line]["target"]
    if line == "postgres-sync":
        up_postgres_sync()
    elif line == "postgres-single":
        up_pg("pg-single", [])
    elif line == "mongo-rs3":
        up_mongo_rs3()
    elif line == "mongo-single":
        up_mongo_single()
    elif t == "fenvix":
        up_fenvix(line)
    else:
        raise ValueError(line)
    log(f"{line}: up ({', '.join(all_containers(line))})")


def check_line(line: str) -> dict:
    """The durability checks of P-bench-local 2.3, before the run (and after the cold restart)."""
    out = {"line": line, "durability": LINES[line]["durability"]}
    if line == "postgres-sync":
        rep = pg_replication()
        out["pg_stat_replication"] = rep
        out["ok"] = (len(rep) == 2 and all(r["state"] == "streaming" and r["sync_state"] == "quorum" for r in rep)
                     and {r["application_name"] for r in rep} == {"standby1", "standby2"})
        out["settings_primary"] = pg_settings(IP["pg-primary"])
        out["ok"] = out["ok"] and out["settings_primary"]["synchronous_standby_names"] == "ANY 1 (standby1, standby2)" \
            and out["settings_primary"]["synchronous_commit"] == "on"
        out["settings_standby1"] = pg_settings(IP["pg-standby1"])
    elif line == "postgres-single":
        out["settings"] = pg_settings(IP["pg-single"])
        out["ok"] = True
    elif line == "mongo-rs3":
        out["ok"] = mongo_rs_ok()
        out["settings"] = mongo_settings("mongo-a")
        out["uri_write_concern"] = "w=majority&journal=true (connection string of equal-durability.json)"
    elif line == "mongo-single":
        out["settings"] = mongo_settings("mongo-single")
        out["ok"] = True
    else:
        from pymongo import MongoClient
        # on this engine the 3-zone log's counters are in the tenantStats command (serverStatus().fenvix has no
        # "quaderno"); the log is configured when "sent", "batches" and "failed" exist there
        ts = MongoClient(uri_of(line), serverSelectionTimeoutMS=10000).admin.command("tenantStats")
        q = ts.get("quaderno")
        out["quaderno"] = q
        out["ok"] = (q is not None) if line == "fenvix-log" else True
    return out


def members_state(line: str) -> dict:
    """docker inspect of every container of the line: running, OOM-killed, restarts, exit code (review F10)."""
    out = {}
    for n in all_containers(line):
        r = sh(["docker", "inspect", "-f", "{{json .State}}|{{.RestartCount}}", n], check=False)
        if r.returncode != 0:
            out[n] = {"present": False}
            continue
        st, rc = r.stdout.strip().rsplit("|", 1)
        st = json.loads(st)
        out[n] = {"present": True, "running": st.get("Running"), "oom_killed": st.get("OOMKilled"),
                  "exit_code": st.get("ExitCode"), "restart_count": int(rc), "status": st.get("Status")}
    return out


def health(line: str) -> dict:
    """bench-local review F10: every member up and never OOM-killed, and the line's durability check still holds. Run by
    bench.py after the write phase and at the end (--health-cmd)."""
    ms = members_state(line)
    bad = [n for n, m in ms.items() if not m.get("present") or not m.get("running") or m.get("oom_killed")]
    try:
        chk = check_line(line)
    except Exception as e:  # noqa: BLE001 - a check that cannot run is a failed check
        chk = {"ok": False, "error": repr(e)}
    return {"ok": not bad and bool(chk.get("ok")), "members": ms, "not_healthy": bad, "check": chk}


def restart(line: str) -> None:
    """The cold phase's restart: every container of the line (MinIO included), then the host's page cache, then wait
    until the line is up again WITHOUT reading the benchmark's database."""
    t0 = time.time()
    order = all_containers(line)
    if LINES[line]["target"] == "fenvix":
        order = ["minio"] + [c for c in order if c.startswith("log-")] + [c for c in order if c in ("cook", "spare", "cook-nolog")]
    elif line == "postgres-sync":
        order = ["pg-primary", "pg-standby1", "pg-standby2"]
    sh(["docker", "restart", "-t", RESTART_T] + order, timeout=900)
    log(f"restart: docker restart {' '.join(order)} in {time.time() - t0:.1f} s")
    r = subprocess.run("sync; echo 3 > /proc/sys/vm/drop_caches", shell=True, capture_output=True, text=True)
    log(f"drop_caches: {'done' if r.returncode == 0 else 'NOT ALLOWED: ' + r.stderr.strip()}")
    if line in ("postgres-sync", "postgres-single"):
        host = IP["pg-primary" if line == "postgres-sync" else "pg-single"]
        wait_until("postgres up", lambda: pg_ready(host), 300)
        if line == "postgres-sync":
            wait_until("standbys streaming quorum", lambda: len(pg_replication()) == 2 and all(
                r["state"] == "streaming" and r["sync_state"] == "quorum" for r in pg_replication()), 300)
    elif line == "mongo-rs3":
        wait_until("rs0 healthy", mongo_rs_ok, 300)
    elif line == "mongo-single":
        wait_until("mongo-single up", lambda: mongo_client("mongo-single").admin.command("ping"), 300)
    else:
        wait_until("our server answers", lambda: ours_ready(line), 600)
        if line == "fenvix-log":
            wait_until("spare listening again", lambda: docker_logs("spare").count("listening on") >= 2, 300)
    c = check_line(line)
    log(f"restart: line up again in {time.time() - t0:.1f} s, check ok={c['ok']}")
    if not c["ok"]:
        raise RuntimeError(f"after restart the durability check fails: {c}")


# ----------------------------------------------------------------------------------------------------------------------
# CPU and memory samplers (host /proc, containers' cgroup v1 files)
# ----------------------------------------------------------------------------------------------------------------------

class Sampler(threading.Thread):
    """Every `every` seconds: host CPU and memory, each container's CPU and memory, the disk's free space. Below
    `min_free_gib` free, `disk_low` is set (the runner then stops the line: a full disk makes MinIO refuse writes)."""

    def __init__(self, line: str, path: str, every: float = 5.0, min_free_gib: float = 1.5):
        super().__init__(daemon=True)
        self.line, self.path, self.every, self.stop = line, path, every, threading.Event()
        self.min_free_gib, self.disk_low, self.disk_min_free_gib = min_free_gib, threading.Event(), None
        self.client_pid = None   # bench.py's pid once started: its CPU and its children's go in "client" (review F7)
        self.tick = os.sysconf("SC_CLK_TCK")

    def _client_cpu_s(self):
        """CPU seconds of bench.py and every process below it (worker processes, children)."""
        if not self.client_pid:
            return None
        todo, seen, total = [self.client_pid], set(), 0.0
        while todo:
            p = todo.pop()
            if p in seen:
                continue
            seen.add(p)
            try:
                f = open(f"/proc/{p}/stat").read().rsplit(")", 1)[1].split()
                total += (int(f[11]) + int(f[12])) / self.tick
                for t in os.listdir(f"/proc/{p}/task"):
                    todo += [int(x) for x in open(f"/proc/{p}/task/{t}/children").read().split()]
            except (OSError, ValueError, IndexError):
                continue
        return total

    @staticmethod
    def _cpu():
        v = list(map(int, open("/proc/stat").readline().split()[1:]))
        return sum(v), v[3] + v[4]

    def run(self):
        ids = {n: sh(["docker", "inspect", "-f", "{{.Id}}", n], check=False).stdout.strip() for n in all_containers(self.line)}
        ids = {n: i for n, i in ids.items() if i}
        prev_t, prev_c = time.time(), {}
        tot, idle = self._cpu()
        prev_client = None
        with open(self.path, "w") as f:
            while not self.stop.wait(self.every):
                now = time.time()
                t2, i2 = self._cpu()
                busy = 1 - (i2 - idle) / max(1, t2 - tot)
                tot, idle = t2, i2
                mi = {l.split(":")[0]: int(l.split()[1]) for l in open("/proc/meminfo")}
                free = shutil.disk_usage("/").free / 2**30
                self.disk_min_free_gib = round(min(free, self.disk_min_free_gib or free), 2)
                if free < self.min_free_gib:
                    self.disk_low.set()
                row = {"t": round(now, 1), "host_cpu_busy": round(busy, 3), "host_cores": os.cpu_count(),
                       "host_mem_used_gib": round((mi["MemTotal"] - mi["MemAvailable"]) / 2**20, 2),
                       "disk_free_gib": round(free, 2), "c": {}, "gone": []}
                cl = self._client_cpu_s()
                if cl is not None and prev_client is not None and cl >= prev_client:
                    row["client_cores"] = round((cl - prev_client) / (now - prev_t), 3)
                prev_client = cl
                for n, cid in ids.items():
                    try:
                        u = int(open(f"/sys/fs/cgroup/cpuacct/docker/{cid}/cpuacct.usage").read())
                        m = int(open(f"/sys/fs/cgroup/memory/docker/{cid}/memory.usage_in_bytes").read())
                    except OSError:
                        row["gone"].append(n)   # its cgroup is gone: the container is not running (review F10)
                        continue
                    if n in prev_c and u >= prev_c[n]:  # the counter starts again at 0 when a container restarts
                        row["c"][n] = {"cores": round((u - prev_c[n]) / 1e9 / (now - prev_t), 3), "mem_mib": round(m / 2**20)}
                    prev_c[n] = u
                prev_t = now
                f.write(json.dumps(row) + "\n")
                f.flush()


def summarize_samples(path: str) -> dict:
    rows = [json.loads(l) for l in open(path) if l.strip()]
    if not rows:
        return {}
    hb = [r["host_cpu_busy"] for r in rows]
    out = {"samples": len(rows), "host_cpu_busy_mean": round(sum(hb) / len(hb), 3), "host_cpu_busy_max": max(hb),
           "host_cpu_busy_ge_95pct_share": round(sum(x >= 0.95 for x in hb) / len(hb), 3),
           "host_mem_used_gib_max": max(r["host_mem_used_gib"] for r in rows), "containers": {}}
    names = sorted({n for r in rows for n in r["c"]})
    for n in names:
        cs = [r["c"][n]["cores"] for r in rows if n in r["c"]]
        ms = [r["c"][n]["mem_mib"] for r in rows if n in r["c"]]
        out["containers"][n] = {"cores_mean": round(sum(cs) / len(cs), 3), "cores_max": max(cs),
                                "at_cap_share": round(sum(x >= 0.95 for x in cs) / len(cs), 3), "mem_mib_max": max(ms)}
    cc = [r["client_cores"] for r in rows if "client_cores" in r]
    if cc:
        out["client_cores_mean"] = round(sum(cc) / len(cc), 3)
        out["client_cores_max"] = max(cc)
    gone = sorted({n for r in rows for n in r.get("gone", [])})
    out["containers_gone_in_some_sample"] = gone   # expected only around the cold restart
    return out


# ----------------------------------------------------------------------------------------------------------------------
# run one line
# ----------------------------------------------------------------------------------------------------------------------

def disk() -> dict:
    u = shutil.disk_usage("/")
    return {"free_gib": round(u.free / 2**30, 1), "used_gib": round(u.used / 2**30, 1)}


def run_line(a) -> int:
    line, out = a.line, os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    os.makedirs(os.path.join(out, "logs"), exist_ok=True)
    down()
    d0 = disk()
    log(f"{line}: disk before {d0}")
    if d0["free_gib"] < a.min_free_gib:
        log(f"STOP: only {d0['free_gib']} GiB free (< {a.min_free_gib})")
        return 4
    settings = {"line": LINES[line], "limits": LIMITS, "minio": "own container, not capped (our lines only)",
                "network": f"docker network {NET} {SUBNET}; client on the host", "disk_before": d0,
                "binaries_sha256": bin_sha() if LINES[line]["target"] == "fenvix" else None}
    t0 = time.time()
    try:
        up(line)
        chk = check_line(line)
        settings["check_before"] = chk
        log(f"{line}: durability check ok={chk['ok']}")
        if not chk["ok"]:
            raise RuntimeError(f"durability check failed: {chk}")
        if LINES[line]["target"] == "fenvix":
            cook = members(line)[3] if line == "fenvix-log" else "cook-nolog"
            settings["server_command"] = sh(["docker", "inspect", "-f", "{{json .Config.Cmd}}", cook]).stdout.strip()
            settings["server_help"] = sh(["docker", "exec", cook, "/opt/fx/mongo-server", "--help"], check=False).stdout
            settings["server_startup_lines"] = docker_logs(cook).splitlines()[:80]
            if line == "fenvix-log":
                settings["log_commands"] = {f"log-{i}": sh(["docker", "inspect", "-f", "{{json .Config.Cmd}}", f"log-{i}"]).stdout.strip() for i in range(3)}
                settings["spare_startup_lines"] = docker_logs("spare").splitlines()[:40]
        json.dump(settings, open(os.path.join(out, "settings.json"), "w"), indent=1, default=str)
        sampler = Sampler(line, os.path.join(out, "cpu_mem.jsonl"))
        sampler.start()
        env = dict(os.environ, BENCH_URI=uri_of(line))
        if LINES[line]["target"] == "fenvix":
            env |= {"BENCH_S3_ENDPOINT": S3["endpoint"], "BENCH_S3_BUCKET": S3["bucket"], "BENCH_S3_KEY": S3["key"],
                    "BENCH_S3_SECRET": S3["secret"], "BENCH_S3_REGION": S3["region"], "NO_PROXY": "*", "no_proxy": "*"}
        else:
            env |= {"NO_PROXY": "*", "no_proxy": "*"}
        cmd = [PY, os.path.join(HERE, "bench.py"), "--local", "--target", LINES[line]["target"],
               "--label", f"local {line}: {LINES[line]['durability']}", "--profile", a.profile, "--seed", str(a.seed),
               "--workers", str(a.workers), "--read-sample", str(a.read_sample), "--erase-users", str(a.erase_users),
               "--out", out, "--settle", "--cold-cmd", f"{PY} {os.path.abspath(__file__)} restart {line}",
               "--cold-threads", str(a.cold_threads),
               "--health-cmd", f"{PY} {os.path.abspath(__file__)} health {line}"]
        if LINES[line]["target"] == "fenvix":
            cmd += ["--engine-sha256", settings["binaries_sha256"][SRV]]
        settings["bench_command"] = " ".join(cmd)
        json.dump(settings, open(os.path.join(out, "settings.json"), "w"), indent=1, default=str)
        log(f"{line}: bench.py starts")
        disk_stop = False
        with open(os.path.join(out, "terminal.txt"), "w") as t:
            proc = subprocess.Popen(cmd, env=env, stdout=t, stderr=subprocess.STDOUT, cwd=HERE)
            sampler.client_pid = proc.pid
            while proc.poll() is None:
                if sampler.disk_low.wait(5):
                    disk_stop = True
                    log(f"{line}: STOP: less than {sampler.min_free_gib} GiB free on the disk; bench.py stopped")
                    proc.terminate()
                    try:
                        proc.wait(60)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    break
            rc = proc.wait()
        sampler.stop.set()
        sampler.join()
        if disk_stop:
            rc = 6
        log(f"{line}: bench.py exit {rc} after {time.time() - t0:.0f} s")
        res = {"bench_rc": rc, "seconds": round(time.time() - t0, 1), "disk_stop": disk_stop,
               "disk_min_free_gib": sampler.disk_min_free_gib, "cpu_mem": summarize_samples(os.path.join(out, "cpu_mem.jsonl"))}
        if LINES[line]["target"] == "fenvix":
            res["binaries_sha256_after"] = bin_sha()
            res["binaries_unchanged"] = res["binaries_sha256_after"] == settings["binaries_sha256"]
            try:
                s = json.load(open(os.path.join(out, "summary.json")))
                b = (s.get("server_status_before") or {}).get("quaderno") or {}
                w = (s.get("server_status_after_write") or {}).get("quaderno") or {}
                res["quaderno_sent_before_after_write"] = [b.get("sent"), w.get("sent")]
                if line == "fenvix-log" and not ((w.get("sent") or 0) > (b.get("sent") or 0)):
                    res["quaderno_check"] = "FAILED: quaderno.sent did not grow during the write phase"
                    rc = rc or 5
                elif line == "fenvix-log":
                    res["quaderno_check"] = "ok: quaderno.sent grew during the write phase"
            except Exception as e:  # noqa: BLE001
                res["quaderno_check"] = f"no summary: {e!r}"
        res["members_end"] = members_state(line)
        if any(m.get("oom_killed") or not m.get("running") for m in res["members_end"].values()):
            res["members_check"] = "FAILED: a member is not running or was OOM-killed"
            rc = rc or 7
        if LINES[line]["target"] == "fenvix":
            try:
                lf = json.load(open(os.path.join(out, "summary.json"))).get("log_failed") or {}
                res["log_failed"] = lf
                if any(lf.values()):
                    res["quaderno_failed_check"] = "FAILED: the 3-zone log refused batches (S3-WAL fallback acknowledged them)"
                    rc = rc or 5
            except Exception as e:  # noqa: BLE001
                res["quaderno_failed_check"] = f"no summary: {e!r}"
        res["disk_end"] = disk()
        json.dump(res, open(os.path.join(out, "line_result.json"), "w"), indent=1)
        return rc
    finally:
        for n in all_containers(line):
            txt = docker_logs(n)
            if txt:
                import gzip
                with gzip.open(os.path.join(out, "logs", f"{n}.log.gz"), "wt") as f:
                    f.write(txt)
        if not a.keep:
            down()
            log(f"{line}: containers and data removed; disk {disk()}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("line", choices=sorted(LINES))
    r.add_argument("--out", required=True)
    r.add_argument("--workers", type=int, default=1)
    r.add_argument("--profile", default=CONF["profile"])
    r.add_argument("--seed", type=int, default=CONF["seed"])
    r.add_argument("--read-sample", type=int, default=300)
    r.add_argument("--erase-users", type=int, default=100)
    r.add_argument("--cold-threads", type=int, default=200)
    r.add_argument("--min-free-gib", type=float, default=12)
    r.add_argument("--keep", action="store_true", help="leave the containers up (debugging only)")
    for c in ("up", "restart", "check", "health"):
        p = sub.add_parser(c)
        p.add_argument("line", choices=sorted(LINES))
    sub.add_parser("down")
    sub.add_parser("hosts")
    a = ap.parse_args()
    if a.cmd == "run":
        sys.exit(run_line(a))
    if a.cmd == "up":
        down()
        up(a.line)
    elif a.cmd == "restart":
        restart(a.line)
    elif a.cmd == "health":
        h = health(a.line)
        print(json.dumps(h, indent=1, default=str))
        sys.exit(0 if h["ok"] else 1)
    elif a.cmd == "check":
        print(json.dumps(check_line(a.line), indent=1, default=str))
    elif a.cmd == "down":
        down()
    elif a.cmd == "hosts":
        hosts()


if __name__ == "__main__":
    main()
