"""Tables of results-round3.md from the JSON of the runs: python3 tools/round3/report.py [minio|real-s3] > tables.md"""
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
kind = sys.argv[1] if len(sys.argv) > 1 else "minio"
D = os.path.join(ROOT, "results-round3", kind)
MODES = ["before", "immediate", "adaptive", "relaxed", "local"]
LABEL = {"before": "before (main)", "immediate": "immediate", "adaptive": "adaptive", "relaxed": "relaxed (5 s)", "local": "local log (30 s)"}
runs = {}
for f in glob.glob(os.path.join(D, "*.json")):
    try:
        r = json.load(open(f))
    except Exception:
        continue
    if "mode" in r and "usd_requests_per_db_month" in r:
        runs[r["name"]] = r
pre = "s3-" if kind == "real-s3" else ""


def get(name):
    return runs.get(pre + name)


def fmt(x, d=2):
    if x is None:
        return "-"
    if isinstance(x, float):
        return f"{x:.{d}f}".replace(".", ",")
    return str(x)


def scen():
    out = []
    for rate in (1, 10, 100):
        for mix in ("half", "write", "read"):
            out.append((f"{rate} req/s, {'50/50' if mix == 'half' else 'only writes' if mix == 'write' else 'only reads'}", lambda m, rate=rate, mix=mix: f"{m}-r{rate}-{mix}" if kind == "minio" else f"{m}-r{rate}"))
    out.append(("fleet: 100 databases, 1 write/s each", lambda m: f"fleet100-{m}"))
    out.append(("mixed fleet: 50 in bursts, 20 reading, 30 idle", lambda m: f"fleetmixed-{m}"))
    return out


def table(metric, title, d=2):
    print(f"\n**{title}**\n")
    print("| scenario | " + " | ".join(LABEL[m] for m in MODES) + " |")
    print("|---|" + "---|" * len(MODES))
    seen = set()
    for label, nm in scen():
        row = [get(nm(m)) for m in MODES]
        if not any(row) or nm("x") in seen:
            continue
        seen.add(nm("x"))
        print(f"| {label} | " + " | ".join(fmt(metric(r), d) if r else "-" for r in row) + " |")


table(lambda r: r["usd_requests_per_db_month"], "Request cost, USD per database per month")
table(lambda r: r["s3_per_client_request"], "S3 requests per client request", 3)
table(lambda r: r["put_per_db_min"], "PUT/LIST-tier requests per database per minute", 1)
table(lambda r: r["get_per_db_min"], "GET-tier requests per database per minute", 1)


def lat(r, k):
    ls = list(r["client"].values())
    v = [l.get(k) for l in ls if l.get(k) is not None]
    return max(v) if v else None


table(lambda r: lat(r, "write_steady_p50_ms"), "Write latency p50, ms (after the first minute)", 1)
table(lambda r: lat(r, "write_steady_p99_ms"), "Write latency p99, ms (after the first minute)", 1)
table(lambda r: lat(r, "read_steady_p50_ms"), "Read latency p50, ms (after the first minute)", 1)
table(lambda r: lat(r, "read_steady_p99_ms"), "Read latency p99, ms (after the first minute)", 1)
table(lambda r: r.get("rss_mb_max"), "Engine RSS max, MB", 0)
table(lambda r: sum(l.get("errors", 0) for l in r["client"].values()), "Client errors", 0)
print("\n**Storage, USD per database per month** (bytes listed at the end of each run)\n")
for nm, r in sorted(runs.items()):
    pass
print("| run | bytes per database | USD per database per month |\n|---|---|---|")
for nm, r in sorted(runs.items()):
    if nm.endswith("-half") and ("-r10-" in nm) or nm.startswith(pre + "fleet100"):
        print(f"| {nm} | {r['storage_bytes'] // max(1, r['dbs'])} | {fmt(r['usd_storage_per_db_month'], 6)} |")
print("\n**By API, per database per minute** (counted window)\n")
print("| run | " + " | ".join(["PutObject", "ListObjectsV2", "GetObject", "HeadObject", "CopyObject", "DeleteMultipleObjects", "DeleteObject"]) + " |")
print("|---|" + "---|" * 7)
for nm, r in sorted(runs.items()):
    w = r["window_s"] / 60 * r["dbs"]
    print(f"| {nm} | " + " | ".join(fmt(r["s3_by_api_window"].get(k, 0) / w, 2) for k in ["PutObject", "ListObjectsV2", "GetObject", "HeadObject", "CopyObject", "DeleteMultipleObjects", "DeleteObject"]) + " |")
