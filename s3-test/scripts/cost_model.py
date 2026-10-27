#!/usr/bin/env python3
"""Monthly cost for N dormant databases, from the measurements of test 3 (test3.json) and the verified prices.

    cost_model.py <test3.json> [prices.json]

Formulas (all USD per month, 730 h, region eu-central-1, prices from prices.json):
  storage   = N * bytes_per_db / 2^30 * storage_usd_per_gb_month
  wakes     = N * w * (wake_put_tier * put_usd + wake_get_tier * get_usd)         w = wakes per database per month
  idle      = nodes * 730 * (idle_put_tier_per_h * put_usd + idle_get_tier_per_h * get_usd)   requests of the node at rest
  compute   = nodes * (730 * c6i_large_usd_per_hour + 8 * gp3_usd_per_gb_month)   8 GiB root volume
  nodes     = h * max(1, ceil(N * f / K_max))      f = fraction of databases awake together, K_max = databases the node keeps
              awake under light load within the limits of test3.py, h = 1 (one node) or 2 (two nodes for availability)
bytes_per_db = S3 bytes per database measured with 1,000 dormant empty databases (the "empty" profile) or the settled
size of the 1,000-document and 100,000-document databases (profiles "1k" / "100k").
NOT included: public IPv4 address charge (price not verified), taxes, data transfer, request retries beyond what the
HTTP-level counters saw, CloudWatch. Valid for N above 1,000 only as an extrapolation (stated in the output)."""
import json
import math
import os
import sys

t3 = json.load(open(sys.argv[1]))
P = json.load(open(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prices.json")))
put_usd, get_usd = P["put_tier_usd_per_request"], P["get_tier_usd_per_request"]
ph = t3["phases"]
stages = ph["stages"]
last = stages[-1]
sizes = ph.get("sizes", {})
wake = ph.get("wake", {})
awake = ph.get("awake", {}).get("levels", [])
ok_levels = [l["databases"] for l in awake if not l["limits_exceeded"]]
kmax = max(ok_levels) if ok_levels else None


def wk(label):
    w = wake.get(label)
    if not w:
        return 0, 0
    r = w["requests_median"]
    return r.get("httpPutTier", r["billedPutTier"]), r.get("httpGetTier", r["billedGetTier"])


profiles = {"empty": (last["bytes_per_db"], "empty_dormant"), "1k docs": (sizes.get("size_1k", {}).get("after_settle", {}).get("bytes"), "docs_1k"),
            "100k docs": (sizes.get("size_100k", {}).get("after_settle", {}).get("bytes"), "docs_100k")}
idle = last["idle_60s"]["requests"]
secs = last["idle_60s"]["secs"]
idle_put_h = idle.get("httpPutTier", idle["billedPutTier"]) / secs * 3600
idle_get_h = idle.get("httpGetTier", idle["billedGetTier"]) / secs * 3600
print(f"measured: bytes/db (empty, {last['databases']} dormant) = {last['bytes_per_db']:.0f}; idle node requests/h: PUT-tier {idle_put_h:.1f}, GET-tier {idle_get_h:.1f}; "
      f"K_max (light load, within limits) = {kmax}; node at rest: CPU {last['idle_60s']['cpu_pct_of_one_core']:.1f}% of a core, RSS {last['idle_60s']['rss_mb']:.0f} MB")
print("N, profile, w (wakes/db/month), f (awake fraction), nodes, storage, wakes, idle, compute, TOTAL  [USD/month]")
rows = []
for N in (5, 10, 50, 100, 1000, 2000):
    for pname, (bytes_db, wlabel) in profiles.items():
        if not bytes_db:
            continue
        wp, wg = wk(wlabel)
        for w in (0, 30):
            for f in (0.0, 0.05):
                for h in (1, 2):
                    nodes = h * max(1, math.ceil(N * f / kmax)) if kmax else None
                    if nodes is None:
                        continue
                    storage = N * bytes_db / 2 ** 30 * P["storage_usd_per_gb_month"]
                    wakes = N * w * (wp * put_usd + wg * get_usd)
                    idle_c = nodes * 730 * (idle_put_h * put_usd + idle_get_h * get_usd)
                    compute = nodes * (730 * P["c6i_large_usd_per_hour"] + 8 * P["gp3_usd_per_gb_month"])
                    rows.append((N, pname, w, f, nodes, storage, wakes, idle_c, compute, storage + wakes + idle_c + compute))
for r in rows:
    print("%5d, %-9s, %2d, %.2f, %d, %.4f, %.4f, %.4f, %.2f, %.2f" % r)
if max(r[0] for r in rows) > last["databases"]:
    print(f"NOTE: N above {last['databases']} is an extrapolation of the per-database figures; only {last['databases']} dormant databases were actually created.")
