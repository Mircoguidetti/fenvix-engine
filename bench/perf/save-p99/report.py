"""save-p99: per-stage p50/p99/max of the paced insert_one (coll t of db sv) from a run's server.log, the client's view,
the leader's log timers, and which stage owns the tail.

  python3 report.py OUT [OUT ...]      (one block per run; --json writes OUT/report.json)
"""
import json
import os
import re
import sys

STAGES = ["admit", "lock_w", "lock_r", "refresh", "stage", "stage_snap", "log_ack", "visible", "fallback", "wal_flush", "io_wait",
          "get", "put", "store_other", "comp_start", "read_get", "read_scan", "rest"]


def q(v, p):
    if not v:
        return 0.0
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))]


def parse(out):
    cmds, msgs, stalls, polls = [], [], [], []
    for line in open(os.path.join(out, "server.log"), errors="replace"):
        if line.startswith("WAITS ") and " cmd=insert " in line and " coll=t " in line:
            d = {"ms": float(re.search(r" ms=([\d.]+)", line).group(1)), "u": float(re.search(r" u=([\d.]+)", line).group(1))}
            for k, v in re.findall(r" (\w+)=([\d.]+)/\d+", line):
                d[k] = float(v)
            m = re.search(r" rest=(-?[\d.]+)", line)
            d["rest"] = float(m.group(1)) if m else 0.0
            # sub-stages added on this branch (save-p99 tracing): key=ms with no /n
            for k, v in re.findall(r" (sv_\w+)=([\d.]+)(?=\s|$)", line):
                d[k] = float(v)
            cmds.append(d)
        elif line.startswith("WAITS_MSG ") and " cmd=insert " in line:
            msgs.append({k: float(v) for k, v in re.findall(r" (ms|handle|write_reply|usage)=(-?[\d.]+)", line)})
        elif line.startswith("WAITS_STALL"):
            stalls.append(float(re.search(r" ms=([\d.]+)", line).group(1)))
        elif line.startswith("WAITS_POLL"):
            polls.append(line.strip())
    return cmds, msgs, stalls, polls


def block(out):
    cmds, msgs, stalls, polls = parse(out)
    # drop the first 10 s (opening, first compactor start)
    if cmds:
        t0 = min(c["u"] for c in cmds) + 10_000
        cmds = [c for c in cmds if c["u"] >= t0]
    res = {"run": out, "n": len(cmds)}
    tot = [c["ms"] for c in cmds]
    res["server_total"] = [q(tot, .5), q(tot, .99), max(tot, default=0)]
    keys = [k for k in STAGES if any(k in c for c in cmds)] + sorted({k for c in cmds for k in c if k.startswith("sv_")})
    res["stages"] = {k: [q([c.get(k, 0.0) for c in cmds], .5), q([c.get(k, 0.0) for c in cmds], .99), max((c.get(k, 0.0) for c in cmds), default=0)] for k in keys}
    # tail: the slowest 1% by server total; mean of each stage there, and how often each stage is the largest
    tail = sorted(cmds, key=lambda c: -c["ms"])[: max(1, len(cmds) // 100)]
    res["tail_n"] = len(tail)
    res["tail_mean"] = {k: sum(c.get(k, 0.0) for c in tail) / len(tail) for k in keys if not k.startswith("sv_")} if tail else {}
    res["tail_mean_sub"] = {k: sum(c.get(k, 0.0) for c in tail) / len(tail) for k in keys if k.startswith("sv_")} if tail else {}
    own = {}
    for c in tail:
        k = max((k for k in keys if not k.startswith("sv_")), key=lambda k: c.get(k, 0.0))
        own[k] = own.get(k, 0) + 1
    res["tail_owner"] = own
    if msgs:
        res["msg_all_inserts"] = {k: [q([m[k] for m in msgs], .5), q([m[k] for m in msgs], .99), max(m[k] for m in msgs)] for k in ("ms", "handle", "write_reply", "usage")}
    cl = os.path.join(out, "client.jsonl")
    if os.path.exists(cl):
        rows = [json.loads(l) for l in open(cl)]
        rows = rows[len(rows) // 60:]           # same warm-up cut (about 10 s of 600)
        s = [r[2] for r in rows]
        f = [r[3] for r in rows]
        res["client_service"] = [q(s, .5), q(s, .99), max(s, default=0)]
        res["client_from_schedule"] = [q(f, .5), q(f, .99), max(f, default=0)]
        res["client_n"] = len(rows)
    res["stalls"] = {"n": len(stalls), "max": max(stalls, default=0), "over_20ms": sum(1 for s in stalls if s >= 20)}
    res["polls_over_10ms"] = len(polls)
    st = os.path.join(out, "pq_status.jsonl")
    if os.path.exists(st):
        for l in open(st):
            try:
                s = json.loads(l)
            except ValueError:
                continue
            if s.get("role") in ("Leader", "leader") or "Leader" in str(s.get("role")):
                try:
                    d = json.loads(s["diag"])
                    res["log_leader_us"] = {"append->fdatasync(leader,first)": d["tappe"]["durevole_primo"], "append->follower_ack(first)": d["tappe"]["copia_primo"],
                                            "append->commit": d["tappe"]["conferma"], "loop_round": d["tappe"]["giro"]}
                except Exception as e:  # noqa: BLE001
                    res["log_leader_err"] = str(e)
    rb = os.path.join(out, "readback.json")
    if os.path.exists(rb):
        res["readback"] = json.load(open(rb))
    return res


def show(r):
    f = lambda v: "/".join(f"{x:.1f}" for x in v)
    print(f"== {r['run']}  n={r['n']}  readback={r.get('readback')}")
    if "client_service" in r:
        print(f"  client insert_one (p50/p99/max ms): service {f(r['client_service'])}   from schedule {f(r['client_from_schedule'])}   n={r['client_n']}")
    print(f"  server total {f(r['server_total'])}")
    for k, v in r["stages"].items():
        print(f"    {k:14s} {f(v)}")
    print(f"  tail (slowest {r['tail_n']}): mean " + " ".join(f"{k}={v:.1f}" for k, v in r["tail_mean"].items() if v >= 0.05))
    if r.get("tail_mean_sub"):
        print("    sub: " + " ".join(f"{k}={v:.1f}" for k, v in r["tail_mean_sub"].items()))
    print(f"  tail owner: {r['tail_owner']}")
    if "msg_all_inserts" in r:
        print("  message (all inserts): " + " ".join(f"{k} {f(v)}" for k, v in r["msg_all_inserts"].items()))
    print(f"  runtime stalls {r['stalls']}  polls>10ms {r['polls_over_10ms']}")
    if "log_leader_us" in r:
        print("  log leader us [p50,p99,p999,max]: " + "  ".join(f"{k} {v}" for k, v in r["log_leader_us"].items()))


if __name__ == "__main__":
    js = "--json" in sys.argv
    for o in [x for x in sys.argv[1:] if not x.startswith("--")]:
        r = block(o)
        show(r)
        if js:
            json.dump(r, open(os.path.join(o, "report.json"), "w"), indent=1)
