#!/usr/bin/env python3
"""Giro 5 (M12): mediane per cartella e lotto dai lotti.jsonl in results/g5-*."""
import glob, json, os, statistics, sys
R = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'results')
for d in sorted(glob.glob(os.path.join(R, 'g5-*/lotti.jsonl'))):
    rs = [json.loads(l) for l in open(d)]
    for b in sorted({r['batch'] for r in rs}):
        xs = [r for r in rs if r['batch'] == b]
        m = lambda f: statistics.median(f(r) for r in xs)
        mm = [round(r['leader_main_us_per_rec'], 1) for r in xs]
        print(f"{os.path.basename(os.path.dirname(d))} lotto {b} (n={len(xs)}): thread principale {m(lambda r: r['leader_main_us_per_rec']):.1f} µs/record {mm}, "
              f"processo capo {m(lambda r: r['leader_proc_us_per_rec']):.1f}, repliche {m(lambda r: statistics.mean(r['follower_us_per_rec'].values())):.1f}, "
              f"record/s {m(lambda r: r['recs_per_s_sampled']):.0f}, main core {m(lambda r: r['leader_main_cores']):.2f}, "
              f"byte/record grezzi {m(lambda r: r['repl_raw_bytes_per_rec']):.0f} sul filo {m(lambda r: r['repl_sent_bytes_per_rec']):.0f}, "
              f"ignoti {sum(r['gen']['unknown'] for r in xs)} rifiuti {sum(r['gen']['rejected'] for r in xs)} conflitti {sum(r['gen']['conflicts'] for r in xs)}")
