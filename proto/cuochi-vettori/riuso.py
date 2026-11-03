"""M20, leva 3: riuso del lavoro tra domande uguali o vicine a pochi secondi (agenti).

Sopra la configurazione usearch i8 + ricalcolo (un pezzo) più veloce a richiamo >= 0,95 (dai risultati di nostri.py).
Cache delle domande degli ultimi 60 s con i loro candidati. Domanda nuova:
  - uguale a una in cache (coseno > 0,99999): risposta della cache;
  - vicina (coseno >= tau) a una o più in cache: ricalcolo esatto solo sull'unione dei loro candidati, niente grafo;
  - altrimenti: ricerca normale, e la metto in cache.
Uso: python3 riuso.py <dati> <indice usearch salvato> <risultati nostri .jsonl> <out .jsonl>
"""
import json, sys, time
import numpy as np
from nostri import load, rescore, to_i8

WINDOW = 60.0


def recall_list(res, gt):
    return [len(set(r[:10]) & set(g[:10])) / 10.0 for r, g in zip(res, gt.tolist())]


def run(idx, base, seq, info, gt, ef, R, C, tau, use_cache, m_near=1, scale=1.0):
    idx.expansion_search = ef
    cache_t, cache_v, cache_c, cache_r = [], [], [], []
    res, kinds = [], []
    c0, w0 = time.thread_time(), time.perf_counter()
    for i in range(len(seq)):
        q = seq[i]
        t = info[i]["t"]
        hit = "miss"
        if use_cache:
            while cache_t and cache_t[0] < t - WINDOW:
                cache_t.pop(0); cache_v.pop(0); cache_c.pop(0); cache_r.pop(0)
            if cache_v:
                sims = np.asarray(cache_v) @ q
                j = int(np.argmax(sims))
                if sims[j] > 0.99999:
                    res.append(cache_r[j]); kinds.append("uguale"); continue
                near = np.nonzero(sims >= tau)[0]
                if len(near):
                    near = near[np.argsort(-sims[near])[:m_near]]
                    cand = np.unique(np.concatenate([cache_c[k] for k in near]))
                    res.append(rescore(base, q, cand)); kinds.append("vicina"); continue
        m = idx.search(to_i8(q, scale), max(R, C), threads=1, exact=False)
        keys = m.keys.astype(np.int64)
        r = rescore(base, q, keys[:R])
        res.append(r); kinds.append(hit)
        if use_cache:
            cache_t.append(t); cache_v.append(q); cache_c.append(keys); cache_r.append(r)
    cpu, wall = time.thread_time() - c0, time.perf_counter() - w0
    rec = recall_list(res, gt)
    out = {"n": len(seq), "cpu_s": cpu, "wall_s": wall, "qps_cpu": len(seq) / cpu, "recall10": float(np.mean(rec))}
    for k in ("uguale", "vicina", "miss"):
        sel = [x for x, kk in zip(rec, kinds) if kk == k]
        out[f"n_{k}"] = len(sel)
        out[f"recall_{k}"] = float(np.mean(sel)) if sel else None
    return out


def drop_app_cache(seq, info, gt):
    """Toglie le ripetizioni esatte che una cache dell'app di 60 s avrebbe preso (M18)."""
    keep, last = [], {}
    for i, x in enumerate(info):
        key = seq[i].tobytes()
        if key in last and x["t"] - last[key] <= WINDOW:
            continue
        last[key] = x["t"]
        keep.append(i)
    keep = np.array(keep)
    return seq[keep], [info[i] for i in keep], gt[keep]


def main():
    from usearch.index import Index
    dd, ipath, nres, out = sys.argv[1:5]
    meta, base, q, gt, gtf, cat = load(dd)
    rows = [json.loads(l) for l in open(nres)]
    cand = [r for r in rows if r["engine"] == "usearch" and r["config"].startswith("i8") and r.get("pieces") == 1
            and r["recall10"] >= 0.95]
    best = max(cand, key=lambda r: r["qps_cpu"])
    ef, R = best["params"]["ef"], best["params"]["R"]
    print("configurazione di partenza", best, flush=True)
    idx = Index.restore(ipath)
    scale = json.load(open(ipath + ".json"))["scale"]
    d = meta["dim"]
    with open(out, "a") as f:
        for tag in ("095", "090"):
            seq = np.fromfile(f"{dd}/seq_{tag}.f32", dtype=np.float32).reshape(-1, d)
            info = json.load(open(f"{dd}/seq_{tag}.json"))
            sgt = np.fromfile(f"{dd}/seq_{tag}_gt.i32", dtype=np.int32).reshape(-1, 100)
            for scen, (s, inf, g) in (("tutte", (seq, info, sgt)), ("dopo cache app", drop_app_cache(seq, info, sgt))):
                # riscaldamento
                run(idx, base, s[:300], inf[:300], g[:300], ef, R, R, 0.95, False, 1, scale)
                b = run(idx, base, s, inf, g, ef, R, R, 1.1, False, 1, scale)
                b.update({"seq": tag, "scenario": scen, "variant": "senza riuso", "ef": ef, "R": R})
                print(json.dumps(b), flush=True); f.write(json.dumps(b) + "\n")
                for C, mn in ((R, 1), (50, 1), (100, 1), (50, 3)):
                    for tau in (0.98, 0.95, 0.92, 0.90, 0.88, 0.85, 0.80):
                        x = run(idx, base, s, inf, g, ef, R, C, tau, True, mn, scale)
                        x.update({"seq": tag, "scenario": scen, "variant": "riuso", "tau": tau, "C": C, "m_near": mn,
                                  "ef": ef, "R": R, "speedup_cpu": b["cpu_s"] / x["cpu_s"]})
                        print(json.dumps(x), flush=True); f.write(json.dumps(x) + "\n")


if __name__ == "__main__":
    main()
