"""Markdown tables for results-density.md from results-density/*/summary.json and latency.csv (printed to stdout)."""
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import RESULTS

P1, P2 = 5.0 / 1e6, 0.40 / 1e6


def th(x):
    return f"{int(x):,}".replace(",", " ")


def lat(name, part, op):
    for r in csv.DictReader(open(os.path.join(RESULTS, name, "latency.csv"))):
        if r["part"] == part and r["op"] == op:
            return r


def f(x, d=1):
    return "-" if x is None or x == "" else (f"{float(x):.{d}f}".replace(".", ","))


def row(name, part):
    s = json.load(open(os.path.join(RESULTS, name, "summary.json")))
    w = s[part]
    n, sp = s["n"], w["span_s"]
    mr = w["minio_requests"]
    permin = lambda k: mr.get(k, 0) / sp * 60 / n
    rd, wr = lat(name, part, "all_reads"), lat(name, part, "all_writes")
    return s, w, (f"| {name} | {n} | {part} {int(w['from_s'])}-{int(w['to_s'])} s | {f(w['client_req_per_s'])} | {f(w['open_tenants_avg'])} (max {w['open_tenants_max']}) | "
                  f"{f(w['rss_mb_avg'])} / {f(w['rss_mb_max'])} | {f(w['engine_cpu_pct_of_1core'])} | {f(w['engine_cpu_ms_per_request'], 2)} | "
                  f"{f(rd['p50_ms'])} / {f(rd['p95_ms'])} / {f(rd['p99_ms'])} | {f(wr['p50_ms'])} / {f(wr['p95_ms'])} / {f(wr['p99_ms'])} | "
                  f"{int(rd['errors']) + int(wr['errors'])} | {w['wakes']} |")


def s3row(name, part):
    s = json.load(open(os.path.join(RESULTS, name, "summary.json")))
    w = s[part]
    n, sp = s["n"], w["span_s"]
    mr = w["minio_requests"]
    pm = lambda k: f(mr.get(k, 0) / sp * 60 / n, 2)
    pdm = w["per_db_month"]
    return (f"| {name} | {pm('GetObject')} | {pm('PutObject')} | {pm('ListObjectsV2')} | {pm('HeadObject')} | {pm('DeleteMultipleObjects')} | "
            f"{f(w['s3_requests_per_client_request'], 2)} | {th(pdm['tier1'])} | {th(pdm['tier2'])} | {f(w['s3_request_cost_per_db_month_usd'], 2)} |")


if __name__ == "__main__":
    runs = sys.argv[1:] or sorted(d for d in os.listdir(RESULTS) if os.path.exists(os.path.join(RESULTS, d, "summary.json")))
    print("| run | N | finestra | richieste client/s | DB aperti (media) | RSS MB media / max | CPU % di 1 core | CPU ms / richiesta | letture p50/p95/p99 ms | scritture p50/p95/p99 ms | errori | aperture (wake) |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in runs:
        for part in ("whole", "steady"):
            print(row(r, part)[2])
    print()
    print("| run (whole) | GET / DB / min | PUT / DB / min | LIST / DB / min | HEAD / DB / min | DeleteObjects / DB / min | S3 per richiesta client | classe PUT / DB / mese | classe GET / DB / mese | $ richieste / DB / mese |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in runs:
        print(s3row(r, "whole"))
