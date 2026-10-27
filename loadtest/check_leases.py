#!/usr/bin/env python3
"""S4 ownership check from the servers' lease logs and the kill/restart events.

Builds, per tenant, the intervals during which each server process held the tenant's lease and the intervals
during which it had the tenant open as a writer, and fails if two different holders' intervals ever overlap.

A holder's interval starts when its `acquired` / `writer_open` line is logged (after the conditional write
returned) and ends at the first of: its `release_begin` / `writer_close` / `lost` / `invalid` line (logged before
the release is written), or the time the process was killed (taken after `wait()`, so the process was dead).
"""
import json
import os
import re
import sys
from collections import defaultdict

LINE = re.compile(r"^LEASE (\w+) tenant=(\S+) holder=(\S+) unix_ms=(\d+) ?(.*)$")


def parse(outdir):
    ev = []
    for name in sorted(os.listdir(outdir)):
        if name.startswith("server-") and name.endswith(".log"):
            for line in open(os.path.join(outdir, name), errors="replace"):
                m = LINE.match(line.strip())
                if m:
                    kind, tenant, holder, ms, rest = m.groups()
                    ev.append((int(ms), kind, tenant, holder, rest))
    kills = []
    p = os.path.join(outdir, "events.jsonl")
    if os.path.exists(p):
        for line in open(p):
            e = json.loads(line)
            if e["event"] == "kill":
                kills.append((e["unix_ms_after"], e["holder"]))
    return sorted(ev), sorted(kills)


def intervals(ev, kills, open_kinds, close_kinds):
    """Per tenant: list of (holder, start, end)."""
    by_tenant = defaultdict(list)
    for t, kind, tenant, holder, rest in ev:
        by_tenant[tenant].append((t, kind, holder, rest))
    out = defaultdict(list)
    inf = float("inf")
    for tenant, items in by_tenant.items():
        open_ = {}
        merged = sorted(items + [(k, "kill", h, "") for k, h in kills], key=lambda x: x[0])
        for t, kind, holder, rest in merged:
            if kind in open_kinds:
                open_.setdefault(holder, t)
            elif kind in close_kinds:
                if holder in open_:
                    out[tenant].append((holder, open_.pop(holder), t, kind))
            elif kind == "kill":
                if holder in open_:
                    out[tenant].append((holder, open_.pop(holder), t, "kill"))
        for holder, s in open_.items():
            out[tenant].append((holder, s, inf, "open"))
    return out


def overlaps(iv):
    bad = []
    for tenant, items in iv.items():
        items = sorted(items, key=lambda x: x[1])
        for i, a in enumerate(items):
            for b in items[i + 1:]:
                if b[1] >= a[2]:
                    break
                if a[0] != b[0]:
                    bad.append((tenant, a, b))
    return bad


def main(outdir):
    ev, kills = parse(outdir)
    from collections import Counter
    lease_iv = intervals(ev, kills, {"acquired"}, {"release_begin", "lost", "invalid"})
    writer_iv = intervals(ev, kills, {"writer_open"}, {"writer_close"})
    lb, wb = overlaps(lease_iv), overlaps(writer_iv)
    holders = sorted({h for _, _, _, h, _ in ev})
    takeovers = [(t, tenant, h, r) for t, k, tenant, h, r in ev if k == "acquired" and "takeover_from=-" not in r]
    unclean = [(t, tenant, h, r) for t, k, tenant, h, r in ev if k == "takeover_wake"]
    # Maximum number of holders with the tenant open as a writer at any instant.
    max_writers = 0
    for tenant, items in writer_iv.items():
        pts = sorted([(s, 1) for _, s, e, _k in items] + [(e, -1) for _, s, e, _k in items if e != float("inf")], key=lambda x: (x[0], x[1]))
        cur = 0
        for _, d in pts:
            cur += d
            max_writers = max(max_writers, cur)
    res = {
        "holders": holders,
        "kills": len(kills),
        "tenants_with_lease_events": len(lease_iv),
        "lease_intervals": sum(len(v) for v in lease_iv.values()),
        "writer_intervals": sum(len(v) for v in writer_iv.values()),
        "lease_overlaps_between_different_holders": len(lb),
        "writer_overlaps_between_different_holders": len(wb),
        "max_concurrent_writer_intervals_per_tenant": max_writers,
        "takeovers_of_expired_leases": len(takeovers),
        "wakes_after_takeover_or_crash": len(unclean),
        "unclean_wal_replay_wakes": sum("unclean=true" in r for _, _, _, r in unclean),
        "takeover_wakes_not_unclean": sum("unclean=false" in r and "takeover_from=-" not in r for _, _, _, r in unclean),
        "lease_overlaps_by_how_the_earlier_holder_ended": dict(Counter(a[3] for _, a, b in lb)),
        "lease_overlap_max_ms": max([min(a[2], b[2]) - b[1] for _, a, b in lb] or [0]),
        "lease_overlap_median_ms": sorted([min(a[2], b[2]) - b[1] for _, a, b in lb] or [0])[len(lb) // 2],
        "examples": [list(map(str, x)) for x in (lb + wb)[:5]],
    }
    print(json.dumps(res, indent=1))
    json.dump(res, open(os.path.join(outdir, "lease_check.json"), "w"), indent=1)
    return 0 if not lb and not wb else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
