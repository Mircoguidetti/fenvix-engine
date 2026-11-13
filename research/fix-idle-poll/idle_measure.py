"""fix-idle-poll: object-store requests of idle OPEN databases, per database and per second.

Usage: python idle_measure.py <tag> <binary> <n_dbs> <idle_secs> <mode>   (mode: default | sealed2-derive | off)
Each database gets 20 upserts (a writer database), then the server is left with no request at all for <idle_secs>.
Counts: serverStatus fenvix.s3 deltas (every request kind) over the idle window, plus, from the FX_WAITS_GETS lines of
the server log, the GETs of the window grouped by object kind. The server's idle close is set above the window, so
every database stays open (an open database with no traffic, as an agent app that writes every few minutes keeps it)."""
import collections, json, os, re, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from lib import Server, make_bucket

tag, binary, n_dbs, idle, mode = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
env = {"FX_WAITS": "600000", "FX_WAITS_GETS": "1"}
if mode == "off":
    env["FENVIX_SEALED_RUNS"] = "off"
elif mode == "sealed2-derive":
    env.update({"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
                "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1", "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_DERIVE": "1",
                "FENVIX_SEALED_CODEC": "zstd"})
else:
    env["FENVIX_SEALED_RUNS"] = ""  # the server's own defaults (sealed runs on, T_pin 30 s, lease ttl 30 s)
    del env["FENVIX_SEALED_RUNS"]
for k in [k for k in os.environ if k.startswith("FENVIX_")]:
    os.environ.pop(k)
bucket = make_bucket("idle-" + tag)
extra = ["--lease-ttl-secs", "30"]
s = Server(bucket, "idle-" + tag, idle=idle + 600, env=env, binary=binary, extra=extra).start()
c = s.client()


def wave(k):
    for d in range(n_dbs):
        db = c[f"app{d:02d}"]
        for i in range(20):
            db.checkpoints.update_one({"thread_id": f"t{i}", "checkpoint_ns": "", "checkpoint_id": f"x{k}"}, {"$set": {"v": i}}, upsert=True)


# two waves 40 s apart: with the server's defaults the first version of our manifest is published after release E1's
# wait (T_pin, 30 s), and the database's compactor starts with it; then 30 s for the writes' own background work
wave(0)
time.sleep(40)
wave(1)
time.sleep(30)


def s3():
    return dict(c.admin.command("serverStatus")["fenvix"]["s3"])


def logpos():
    return os.path.getsize(s.log_path)


a, p0, t0 = s3(), logpos(), time.time()
series = []
step = 10
while time.time() - t0 < idle:
    b = s3()
    time.sleep(step)
    e = s3()
    series.append(sum(e.get(k, 0) - b.get(k, 0) for k in ("get", "getRange")))
z, p1, dt = s3(), logpos(), time.time() - t0
delta = {k: z[k] - a.get(k, 0) for k in z if isinstance(z[k], (int, float)) and z[k] - a.get(k, 0)}
with open(s.log_path, "rb") as f:
    f.seek(p0)
    txt = f.read(p1 - p0).decode(errors="replace")
kinds, lists = collections.Counter(), collections.Counter()


def norm(p):
    p = re.sub(r"app\d\d", "appNN", p)
    return re.sub(r"\d{6,}|[0-9A-Z]{26}", "N", p)


# every request passes two counting layers (the process's and the database's), each printing its line: a line with the
# same path less than 1 ms after the previous one of that path is the same request
last = {}
for m in re.finditer(r"WAITS_(GET|LIST) u=([\d.]+) (?:.*? path=(\S+)|prefix=(\S*)) in_cmd=(\w+)", txt):
    kind, u, path = m.group(1), float(m.group(2)), m.group(3) if m.group(1) == "GET" else m.group(4)
    key = (kind, path)
    if key in last and abs(u - last[key]) < 1.0:
        continue
    last[key] = u
    (kinds if kind == "GET" else lists)[norm(path)] += 1
gets = delta.get("get", 0) + delta.get("getRange", 0)
started = len(re.findall(r"FXC not started", open(s.log_path, errors="replace").read()))
out = {"fxc_not_started_lines": started, "tag": tag, "binary": binary, "mode": mode, "dbs": n_dbs, "secs": round(dt, 1), "s3_delta": delta,
       "gets_per_s_per_db": round(gets / dt / n_dbs, 4), "requests_per_s_per_db": round(sum(v for k, v in delta.items() if not k.lower().endswith("bytes")) / dt / n_dbs, 4),
       "gets_per_10s_window_all_dbs": series, "get_kinds": kinds.most_common(30), "list_kinds": lists.most_common(30)}
print(json.dumps(out, indent=1))
os.makedirs("research/fix-idle-poll/out", exist_ok=True)
json.dump(out, open(f"research/fix-idle-poll/out/{tag}.json", "w"), indent=1)
c.close(); s.term()
