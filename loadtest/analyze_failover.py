#!/usr/bin/env python3
"""Failover time from an S4 run: for every SIGKILL, the tenants whose lease the killed process held at that moment, and
the time from the kill to the first successful operation on the same tenant (ops.jsonl, written by loadgen --oplog).

Each tenant sends one operation per second, so the literal definition (kill to first successful operation) includes
up to a second of waiting for the tenant's next operation; `unavailable_ms` removes it by measuring from the start of
the first operation that began after the kill... only operations that started before the kill and finished after it
are kept as is. Both are reported."""
import json, os, sys, statistics
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_leases as cl

def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))] if v else None

def main(d):
    ev, kills_ev = cl.parse(d)
    kills = [json.loads(l) for l in open(os.path.join(d, "events.jsonl")) if '"kill"' in l]
    lease_iv = cl.intervals(ev, [(k["unix_ms_after"], k["holder"]) for k in kills], {"acquired"}, {"release_begin", "lost", "invalid"})
    ops = {}
    for line in open(os.path.join(d, "ops.jsonl")):
        o = json.loads(line)
        ops.setdefault(o["t"], []).append(o)
    for v in ops.values():
        v.sort(key=lambda o: o["e"])
    lit, gap, none_after, pairs = [], [], 0, 0
    per_kill = []
    for k in kills:
        t_kill = k["unix_ms_after"]
        got = []
        for tenant, items in lease_iv.items():
            for holder, s, e, how in items:
                if holder == k["holder"] and how == "kill" and e == t_kill:
                    pairs += 1
                    nxt = [o for o in ops.get(tenant, []) if o["ok"] and o["e"] > t_kill]
                    if not nxt:
                        none_after += 1
                        continue
                    o = nxt[0]
                    lit.append(o["e"] - t_kill)
                    # operations that were already in flight (started before the kill) measure the outage directly
                    gap.append(o["e"] - max(o["s"], t_kill))
                    got.append(o["e"] - t_kill)
        per_kill.append({"holder": k["holder"], "kill_unix_ms": t_kill, "tenants": len(got), "p50_ms": pct(got, .5), "max_ms": max(got) if got else None})
    res = {"kills": len(kills), "tenant_kill_pairs": pairs, "pairs_with_no_successful_op_afterwards": none_after,
           "kill_to_first_success_ms": {"n": len(lit), "p50": pct(lit, .5), "p90": pct(lit, .9), "p99": pct(lit, .99), "max": max(lit) if lit else None},
           "service_time_of_first_success_ms": {"p50": pct(gap, .5), "p99": pct(gap, .99), "max": max(gap) if gap else None},
           "per_kill": per_kill}
    print(json.dumps(res, indent=1))
    json.dump(res, open(os.path.join(d, "failover.json"), "w"), indent=1)

if __name__ == "__main__":
    main(sys.argv[1])
