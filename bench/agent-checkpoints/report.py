"""Print the comparison of one results directory (one sub-directory per target), every number labelled.

Usage: python report.py results/local-2026-11-09
"""
from __future__ import annotations

import json
import os
import sys

OPS = [("save_step", "save one step (put_writes + put)"), ("resume[write/first]", "resume, new thread"),
       ("resume[write/none]", "resume, next turn"), ("resume[write/short]", "resume after short gap"),
       ("resume[write/long]", "resume after long gap"), ("resume[read]", "resume (load latest), read phase"),
       ("time_travel", "time travel to step k"), ("list_limit10", "list last 10"), ("list", "list whole history"),
       ("erase_user", "erase one end user (all threads)")]


def main(d: str) -> None:
    local = "local-" in os.path.basename(os.path.normpath(d))
    label = "MEASURED locally, VALIDATION NOT RESULTS" if local else "MEASURED"
    print(f"Agent checkpoint benchmark, {d}\nAll latencies: {label}; ms, client included.\n")
    for t in ("fenvix", "mongo", "postgres"):
        f = os.path.join(d, t, "summary.json")
        if not os.path.exists(f):
            print(f"[{t}] missing\n")
            continue
        s = json.load(open(f))
        b, L = s["bytes"], s["latency_ms"]
        print(f"[{t}] {s['label']}: ok={s['ok']} threads={s['plan']['threads']} turns={s['plan']['turns']} "
              f"checkpoints={b['checkpoints']} write {s['write']['seconds']} s ({s['write']['steps_per_second']} steps/s, 1 client)")
        for k, name in OPS:
            if k in L:
                x = L[k]
                print(f"  {name}: p50 {x['p50']} / p99 {x['p99']} ms (n={x['n']})")
        print(f"  logical bytes {b['logical_total']:,}; stored {b['stored'] if b['stored'] is None else format(int(b['stored']), ',')} "
              f"({b['stored_from']}) -> stored/logical {b['stored_per_logical']}")
        print(f"  unique message bytes (lower bound of a perfect delta) {b['unique_message_bytes']:,} "
              f"= {b['unique_message_bytes'] / b['logical_total']:.4f} of logical")
        if t == "fenvix":
            for name in ("s3_after_idle_close.json", "s3_after_stop.json"):
                p = os.path.join(d, t, name)
                if os.path.exists(p):
                    s3 = json.load(open(p))
                    print(f"  S3 bytes {name[:-5]}: {s3['total']:,} -> stored/logical {s3['total'] / b['logical_total']:.4f}; by kind {s3['by_kind']}")
            print(f"  S3 PUTs during write: {s.get('s3_put_requests_during_write')} for {b['checkpoints']} checkpoints")
        dt = s.get("delta_test", {})
        print(f"  agent checkpoint delta test: {dt.get('threads')} threads, stored/logical per thread "
              f"{dt.get('ratio_stored_per_logical', 'UNKNOWN')} ({dt.get('stored_bytes_source')})")
        sa = s["sizes_after_erase"].get("stored_bytes")
        print(f"  after erasing {s['erase']['users_erased']} users ({s['erase']['threads_erased']} threads): stored "
              f"{sa if sa is None else format(int(sa), ',')}; threads still readable {s['erase']['threads_left_readable']}\n")
    for t in ("mastra-fenvix", "mastra-mongo", "mastra-postgres"):
        f = os.path.join(d, t, "summary.json")
        if not os.path.exists(f):
            continue
        s = json.load(open(f))
        L = s["latency_ms"]
        print(f"[{t}] ok={s['ok']} messages={s['messages']} (append-only, no per-step full copy) write {s['write_seconds']} s")
        for k in ("save_step", "resume[none]", "resume[long]", "resume_last40[none]", "list", "erase_user"):
            if k in L:
                print(f"  {k}: p50 {L[k]['p50']} / p99 {L[k]['p99']} ms (n={L[k]['n']})")
        print(f"  logical message bytes {s['logical_message_bytes']:,}")
        for name in ("sizes.json", "s3_after_stop.json"):
            p = os.path.join(d, t, name)
            if os.path.exists(p):
                print(f"  {name[:-5]} (after erasing {s['erase']['users']} users): {open(p).read().strip()}")
        print()
    c = os.path.join(d, "cost.txt")
    if os.path.exists(c):
        print("Cost (cost.txt; storage per checkpoint from the local run, prices DECLARED/UNKNOWN):")
        print(open(c).read())


if __name__ == "__main__":
    main(sys.argv[1])
