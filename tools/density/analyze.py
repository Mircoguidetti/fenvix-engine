"""Turn the raw files of a run (WORK/runs/<name>) into CSVs under results-density/<name>/ and one summary row.
  timeseries.csv  one row per 10 s sample: engine RSS, CPU % (of one core), open tenants, engine and MinIO requests in
                  the 10 s before the sample (scaled to per minute), client requests and latency percentiles in that window
  latency.csv     per request type: count, errors, p50/p95/p99/max, over the whole run and over the steady part
  summary.json    the figures of the report, each with how it is computed
usage: analyze.py NAME [STEADY_FROM_S]"""
import csv
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa

MONTH_S = 30 * 86400
# S3 Standard US East prices (as given in the brief): PUT/COPY/POST/LIST $5 per million, GET/HEAD and the rest $0.40 per
# million, DELETE free. DeleteObjects (multi-object delete, an HTTP POST) is taken as a DELETE (free); the upper bound
# that bills it as a POST is reported too.
TIER1 = {"PutObject", "CopyObject", "ListObjectsV2", "ListObjectsV1", "ListObjectVersions", "NewMultipartUpload", "PutObjectPart",
         "CompleteMultipartUpload", "PostPolicyBucket", "PutBucket", "ListBuckets", "ListMultipartUploads"}
FREE = {"DeleteObject", "DeleteMultipleObjects", "AbortMultipartUpload"}
P1, P2, PGB = 5.0 / 1e6, 0.40 / 1e6, 0.023


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(p / 100 * (len(xs) - 1)))))
    return xs[k]


def load(name):
    d = os.path.join(WORK, "runs", name)
    meta = dict(l.strip().split("=", 1) for l in open(os.path.join(d, "meta.txt")) if "=" in l)
    t_start = float([l for l in open(os.path.join(d, "meta.txt")) if l.startswith("T_START")][0].split()[1])
    rows = []
    for f in sorted(glob.glob(os.path.join(d, "lat.p*.csv"))):
        with open(f) as fh:
            for r in csv.DictReader(fh):
                rows.append((float(r["t"]), int(r["db"]), r["op"], float(r["lat_ms"]), int(r["ok"]), float(r["late_ms"]), r["err"]))
    samples = [json.loads(l) for l in open(os.path.join(d, "samples.jsonl"))]
    return d, meta, t_start, rows, samples


LEASE_RE = __import__("re").compile(r"^LEASE (\S+) tenant=(\S+) holder=\S+ unix_ms=(\d+)(.*)$")


def lease_events(name):
    """(unix_s, event, tenant, extra) from the engine log: `acquired` (a tenant is opened, reader or writer, its lease
    taken), `release` (closed), `lost`, `writer_open` / `writer_close`."""
    ev = []
    with open(os.path.join(WORK, "logs", f"engine-{name}.log"), errors="replace") as f:
        for l in f:
            m = LEASE_RE.match(l.strip())
            if m and m.group(2) != "-":
                ev.append((int(m.group(3)) / 1000, m.group(1), m.group(2), m.group(4)))
    ev.sort()
    return ev


class OpenTracker:
    def __init__(self, ev):
        self.ev, self.i, self.open, self.writers = ev, 0, set(), set()
        self.wakes = self.closes = 0

    def at(self, ts):
        while self.i < len(self.ev) and self.ev[self.i][0] <= ts:
            _, e, t, x = self.ev[self.i]
            if e == "acquired":
                if t not in self.open:
                    self.wakes += 1
                self.open.add(t)
            elif e in ("release", "lost"):
                if t in self.open:
                    self.closes += 1
                self.open.discard(t)
                self.writers.discard(t)
            elif e == "writer_open":
                self.writers.add(t)
            elif e == "writer_close":
                self.writers.discard(t)
            self.i += 1
        return len(self.open - {"admin"}), len(self.writers - {"admin"}), self.wakes, self.closes


def tiers_minio(m):
    t1 = sum(v for k, v in m.items() if k in TIER1)
    fr = sum(v for k, v in m.items() if k in FREE)
    multi = m.get("DeleteMultipleObjects", 0)
    t2 = sum(v for k, v in m.items() if k not in TIER1 and k not in FREE)
    return t1, t2, fr, multi


def diff(a, b):
    return {k: b.get(k, 0) - a.get(k, 0) for k in set(a) | set(b)}


def main():
    name = sys.argv[1]
    steady_from = float(sys.argv[2]) if len(sys.argv) > 2 else 60.0
    d, meta, t0, rows, samples = load(name)
    n, dur = int(meta["n"]), float(meta["duration"])
    out = os.path.join(RESULTS, name)
    os.makedirs(out, exist_ok=True)
    samples = [s for s in samples if "rss_mb" in s]
    tr = OpenTracker(lease_events(name))
    for smp in samples:
        smp["open"], smp["writers"], smp["wakes"], smp["closes"] = tr.at(smp["ts"])
    # time series
    ops = ["read_id", "read_idx", "update", "insert"]
    with open(os.path.join(out, "timeseries.csv"), "w", newline="") as f:
        w = csv.writer(f)
        mn_keys = sorted({k for s in samples for k in (s.get("minio") or {}) if k != "error"})
        hdr = ["t_s", "rss_mb", "cpu_pct_of_1core", "threads", "fds", "open_dbs", "open_writers", "wakes_in_window", "closes_in_window"] + \
              [f"minio_{k}_per_min" for k in mn_keys] + ["client_req", "client_err", "client_late_gt1s"] + \
              [f"{o}_{p}_ms" for o in ops for p in ("p50", "p95", "p99")]
        w.writerow(hdr)
        for a, b in zip(samples, samples[1:]):
            dt = b["ts"] - a["ts"]
            ta, tb = a["ts"] - t0, b["ts"] - t0
            win = [r for r in rows if ta <= r[0] < tb]
            mm = diff(a.get("minio") or {}, b.get("minio") or {})
            row = [round(tb, 1), round(b["rss_mb"], 1), round((b["cpu_s"] - a["cpu_s"]) / dt * 100, 1), b["threads"], b["fds"],
                   b["open"], b["writers"], b["wakes"] - a["wakes"], b["closes"] - a["closes"]] + \
                  [round(mm.get(k, 0) * 60 / dt, 1) for k in mn_keys] + \
                  [len(win), sum(1 for r in win if not r[4]), sum(1 for r in win if r[5] > 1000)]
            for o in ops:
                ls = [r[3] for r in win if r[2] == o and r[4]]
                row += [pct(ls, 50), pct(ls, 95), pct(ls, 99)]
            w.writerow(row)
    # latency summary
    with open(os.path.join(out, "latency.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["part", "op", "count", "errors", "p50_ms", "p95_ms", "p99_ms", "max_ms", "late_gt1s"])
        for part, sel in (("whole", lambda r: True), ("steady", lambda r: r[0] >= steady_from)):
            for o in ops + ["all_reads", "all_writes", "all"]:
                rs = [r for r in rows if sel(r) and (r[2] == o or o == "all" or (o == "all_reads" and r[2].startswith("read")) or (o == "all_writes" and not r[2].startswith("read")))]
                ls = [r[3] for r in rs if r[4]]
                w.writerow([part, o, len(rs), sum(1 for r in rs if not r[4]), pct(ls, 50), pct(ls, 95), pct(ls, 99), max(ls) if ls else None,
                            sum(1 for r in rs if r[5] > 1000)])
    errs = {}
    for r in rows:
        if not r[4]:
            errs[r[6][:60]] = errs.get(r[6][:60], 0) + 1
    # summary over a window [a, b] of samples
    def window(lo, hi):
        ss = [s for s in samples if lo - 5 <= s["ts"] - t0 <= hi + 5]
        if len(ss) < 2:
            return None
        a, b = ss[0], ss[-1]
        span = b["ts"] - a["ts"]
        nreq = sum(1 for r in rows if a["ts"] - t0 <= r[0] < b["ts"] - t0)
        cpu = b["cpu_s"] - a["cpu_s"]
        mm = diff(a.get("minio") or {}, b.get("minio") or {})
        t1, t2, fr, multi = tiers_minio(mm)
        opens = [s["open"] for s in ss]
        rss = [s["rss_mb"] for s in ss]
        res = {
            "from_s": round(a["ts"] - t0, 1), "to_s": round(b["ts"] - t0, 1), "span_s": round(span, 1),
            "client_requests": nreq, "client_req_per_s": round(nreq / span, 2),
            "engine_cpu_s": round(cpu, 2), "engine_cpu_pct_of_1core": round(cpu / span * 100, 1),
            "engine_cpu_ms_per_request": round(cpu * 1000 / nreq, 3) if nreq else None,
            "rss_mb_avg": round(sum(rss) / len(rss), 1), "rss_mb_max": round(max(rss), 1),
            "open_tenants_avg": round(sum(opens) / len(opens), 1), "open_tenants_max": max(opens),
            "wakes": b["wakes"] - a["wakes"], "closes": b["closes"] - a["closes"], "open_writers_avg": round(sum(s["writers"] for s in ss) / len(ss), 1),
            "minio_requests": {k: v for k, v in mm.items() if v},
            "minio_tier1_put_class": t1, "minio_tier2_get_class": t2, "minio_free_delete": fr, "minio_delete_multi": multi,
            "minio_total": t1 + t2 + fr,
        }
        per_db_month = lambda x: x / span * MONTH_S / n
        res["per_db_month"] = {"tier1": round(per_db_month(t1)), "tier2": round(per_db_month(t2)), "free": round(per_db_month(fr)),
                               "formula": "requests_in_window / window_seconds * 2592000 / N (30-day month, MinIO counters)"}
        res["s3_request_cost_per_db_month_usd"] = round(per_db_month(t1) * P1 + per_db_month(t2) * P2, 4)
        res["s3_request_cost_per_db_month_usd_multidelete_as_post"] = round((per_db_month(t1) + per_db_month(multi)) * P1 + per_db_month(t2) * P2, 4)
        res["s3_requests_per_client_request"] = round((t1 + t2 + fr) / nreq, 2) if nreq else None
        return res
    base = samples[0]
    summ = {
        "name": name, "mode": meta["mode"], "n": n, "duration_s": dur, "extra": meta.get("extra", ""),
        "engine_exit": meta.get("engine_exit"), "engine_cmd": meta.get("engine_cmd"),
        "rss_mb_at_start_no_tenant_open": round(base["rss_mb"], 1),
        "whole": window(0, dur), "steady": window(steady_from, dur),
        "client_errors": errs,
        "notes": "steady = from STEADY_FROM_S after the load start to its end; whole = the full load window, cold wakes included",
    }
    st = summ["steady"]
    if st and st["open_tenants_avg"]:
        summ["mem_mb_per_open_db"] = round((st["rss_mb_avg"] - summ["rss_mb_at_start_no_tenant_open"]) / st["open_tenants_avg"], 3)
        summ["mem_formula"] = "(average RSS in the steady part - RSS before any tenant was opened) / average open tenants"
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(summ, f, indent=1, sort_keys=True)
    print(json.dumps({k: summ[k] for k in ("name", "n", "mem_mb_per_open_db") if k in summ}),
          json.dumps({k: (summ["steady"] or {}).get(k) for k in ("client_req_per_s", "engine_cpu_pct_of_1core", "engine_cpu_ms_per_request", "rss_mb_avg", "open_tenants_avg", "minio_total", "wakes", "s3_requests_per_client_request", "s3_request_cost_per_db_month_usd")}))


if __name__ == "__main__":
    main()
