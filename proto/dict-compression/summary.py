#!/usr/bin/env python3
"""Readable summary of results/raw.jsonl and results/extras.jsonl -> results/summary.txt.
Run: python3 -I summary.py"""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))
rows = []
for f in ("raw.jsonl", "extras.jsonl"):
    p = os.path.join(HERE, "results", f)
    if os.path.exists(p):
        rows += [json.loads(l) for l in open(p)]

def get(kind, mode, codec, dict_bytes, frame="lean", set_="measure"):
    for r in rows:
        if r.get("kind") == kind and r.get("set") == set_ and r.get("mode") == mode and r.get("codec") == codec and r.get("dict_bytes") == dict_bytes and r.get("frame") == frame:
            return r
    return None

out = []
P = out.append
run = next(r for r in rows if r.get("stat") == "run")
P(f"CPU: {run['cpu']} (one core, taskset), zstd {run['zstd_version']}, median of {run['reps']} repetitions. All numbers MEASURED.")
D = [0, 32768, 65536, 114688]
for kind in ("chat", "agent", "orders", "agent_emb_bin", "agent_emb_arr", "agent_per_domain", "agent_train50k"):
    st = next((r for r in rows if r.get("kind") == kind and r.get("stat") == "sizes"), None)
    P("")
    P(f"== {kind}" + (f": mean {st['mean_bson_bytes']:.0f} B BSON, p50 {st['p50']}, p90 {st['p90']}, p99 {st['p99']}" if st else ""))
    for r in rows:
        if r.get("kind") == kind and r.get("stat") == "dictionary":
            P(f"   dictionary {r['max_bytes']//1024} KB: {r['bytes']} bytes, trained on {st['train_records'] if st else '?'} docs in {r['train_seconds']:.2f} s")
        if r.get("kind") == kind and r.get("stat") == "random-part":
            P(f"   random part: {100*r['random_share_of_raw']:.1f}% of raw bytes are embedding values + 128-bit ids; embedding values alone zstd-1 {r['embedding_values_alone_zstd1_ratio']:.3f}, ids alone {r['ids_alone_zstd1_ratio']:.3f}")
    for mode in ("single", "batch16", "block64k"):
        for codec in ("zstd1", "zstd3"):
            line = []
            for d in D:
                r = get(kind, mode, codec, d)
                if r:
                    s = get(kind, mode, codec, d, "standard")
                    line.append(f"{'none' if d == 0 else str(d//1024)+'K'} {r['ratio']:.3f}" + (f" (std frame {s['ratio']:.3f})" if s else "") + f" [{r['comp_us_per_record']:.2f}+{r['dec_us_per_record']:.2f} us]")
            if line:
                P(f"   {mode:8} {codec}: " + "; ".join(line))
        r = get(kind, mode, "lz4", 0, "block")
        if r:
            P(f"   {mode:8} lz4  : {r['ratio']:.3f} [{r['comp_us_per_record']:.2f}+{r['dec_us_per_record']:.2f} us, {r['comp_mb_s']:.0f} MB/s comp, {r['dec_mb_s']:.0f} MB/s dec]")
    sets = sorted({r["set"] for r in rows if r.get("kind") == kind and r.get("set") not in (None, "measure")})
    for s_ in sets:
        line = []
        for codec in ("zstd1", "zstd3"):
            for d in D:
                r = get(kind, "single", codec, d, set_=s_)
                if r:
                    line.append(f"{codec} {'none' if d == 0 else str(d//1024)+'K'} {r['ratio']:.3f}")
        b0, b1 = get(kind, "block64k", "zstd1", 0, set_=s_), get(kind, "block64k", "zstd1", 65536, set_=s_)
        P(f"   drift {s_}: single " + "; ".join(line) + (f" | block64k zstd1 none {b0['ratio']:.3f}, 64K {b1['ratio']:.3f}" if b0 and b1 else ""))
text = "\n".join(out)
print(text)
open(os.path.join(HERE, "results", "summary.txt"), "w").write(text + "\n")
