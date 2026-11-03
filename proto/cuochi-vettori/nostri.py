"""M20: le nostre leve 1 e 2 (indice in un pezzo, distanze int8/binarie SIMD, ricalcolo sui migliori).

Uso: python3 nostri.py <cartella dati> <file risultati .jsonl> [quali...]
quali: usearch_i8 usearch_b1 usearch_i8_8pezzi faiss_sq8 faiss_sq8_filtro (predefinito: tutti)
Una domanda alla volta, un thread di ricerca. Ricerche per vCPU = domande / secondi di CPU del thread.
"""
import json, os, sys, time
import numpy as np

D = None
FILTER_CAT = 3


def load(dd):
    meta = json.load(open(f"{dd}/meta.json"))
    n, d, nq = meta["n"], meta["dim"], meta["nq"]
    base = np.memmap(f"{dd}/base.f32", dtype=np.float32, mode="r", shape=(n, d))
    q = np.fromfile(f"{dd}/queries.f32", dtype=np.float32).reshape(nq, d)
    gt = np.fromfile(f"{dd}/gt.i32", dtype=np.int32).reshape(nq, 100)
    gtf = np.fromfile(f"{dd}/gtf.i32", dtype=np.int32).reshape(nq, 100)
    cat = np.fromfile(f"{dd}/cat.u8", dtype=np.uint8)
    return meta, base, q, gt, gtf, cat


def recall10(res, gt):
    return float(np.mean([len(set(r[:10]) & set(g[:10])) / 10.0 for r, g in zip(res, gt.tolist())]))


def rescore(base, q, cand, k=10):
    cand = cand[cand >= 0]
    if len(cand) == 0:
        return []
    srt = np.sort(cand)
    sc = base[srt] @ q
    if len(srt) <= k:
        return srt[np.argsort(-sc)].tolist()
    top = np.argpartition(-sc, k - 1)[:k]
    return srt[top[np.argsort(-sc[top])]].tolist()


def timed(fn, queries):
    """Ritorna (risultati, secondi CPU del thread, secondi orologio)."""
    c0, w0 = time.thread_time(), time.perf_counter()
    res = [fn(x) for x in queries]
    return res, time.thread_time() - c0, time.perf_counter() - w0


def sweep(out, engine, config, points, make_fn, queries, gt, filt, extra=None, stop_at=0.985):
    """points: lista di parametri (dict). Riscaldamento sul primo, poi una passata per punto;
    i punti sopra 0,90 e 0,95 rimisurati 2 volte (si tiene il più veloce)."""
    fn = make_fn(points[0])
    timed(fn, queries)  # riscaldamento
    rows = []
    for p in points:
        fn = make_fn(p)
        res, cpu, wall = timed(fn, queries)
        r = recall10(res, gt)
        row = {"engine": engine, "config": config, "filter": filt, "params": p, "recall10": r,
               "qps_cpu": len(queries) / cpu, "qps_wall": len(queries) / wall, "nq": len(queries)}
        if extra:
            row.update(extra)
        rows.append(row)
        print(json.dumps(row), flush=True)
        if r >= stop_at:
            break
    # rimisura dei punti decisivi
    for target in (0.90, 0.95):
        ok = [x for x in rows if x["recall10"] >= target]
        if not ok:
            continue
        best = max(ok, key=lambda x: x["qps_cpu"])
        fn = make_fn(best["params"])
        for _ in range(2):
            _, cpu, wall = timed(fn, queries)
            best["qps_cpu"] = max(best["qps_cpu"], len(queries) / cpu)
            best["qps_wall"] = max(best["qps_wall"], len(queries) / wall)
        best["confirmed"] = True
    with open(out, "a") as f:
        for x in rows:
            f.write(json.dumps(x) + "\n")
    return rows


# ---------------- usearch ----------------

def build_usearch(base, dtype, keys_from=0, n=None, threads=2):
    from usearch.index import Index
    n = base.shape[0] if n is None else n
    d = base.shape[1]
    if dtype == "b1":
        idx = Index(ndim=d, metric="hamming", dtype="b1", connectivity=16, expansion_add=100)
    else:
        idx = Index(ndim=d, metric="ip", dtype=dtype, connectivity=16, expansion_add=100)
    step = 50_000
    t0 = time.time()
    for s in range(keys_from, keys_from + n, step):
        e = min(s + step, keys_from + n)
        x = np.asarray(base[s:e])
        if dtype == "b1":
            x = np.packbits(x > 0, axis=1)
        idx.add(np.arange(s, e, dtype=np.uint64), x, threads=threads)
    return idx, time.time() - t0


def run_usearch(out, base, q, gt, dtype, save=None):
    idx, bt = build_usearch(base, dtype)
    print(f"usearch {dtype} costruito in {bt:.0f}s, hw={idx.hardware_acceleration}", flush=True)
    if save:
        idx.save(save)
    if dtype == "b1":
        qq = np.packbits(q > 0, axis=1)
        efs = [32, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536]
        rs = [40, 80, 160, 320, 640]
    else:
        qq = q
        efs = [16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512]
        rs = [10, 20, 40, 80]
    points = [{"ef": ef, "R": r} for ef in efs for r in rs if r <= max(ef, 10)]
    # ordino per costo atteso, così lo stop a 0,985 ha senso per ogni R
    rows = []
    for r in rs:
        pts = [p for p in points if p["R"] == r]
        if not pts:
            continue

        def make(p, idx=idx):
            idx.expansion_search = p["ef"]
            R = p["R"]

            def fn(i):
                m = idx.search(qq[i], R, threads=1, exact=False)
                return rescore(base, q[i], m.keys.astype(np.int64))
            return fn
        rows += sweep(out, "usearch", f"{dtype}+ricalcolo, 1 pezzo", pts, make, range(len(q)), gt, False,
                      extra={"build_s": bt, "pieces": 1})
    del idx
    return rows


def run_usearch_pieces(out, base, q, gt, pieces=8):
    n = base.shape[0]
    per = n // pieces
    idxs, bt = [], 0.0
    for p in range(pieces):
        ix, t = build_usearch(base, "i8", keys_from=p * per, n=per if p < pieces - 1 else n - p * per)
        idxs.append(ix)
        bt += t
    print(f"usearch i8 in {pieces} pezzi costruito in {bt:.0f}s", flush=True)
    efs = [16, 24, 32, 48, 64, 96, 128, 192, 256]
    rows = []
    for r in (10, 20, 40):
        pts = [{"ef": ef, "R": r} for ef in efs if r <= max(ef, 10)]

        def make(p):
            for ix in idxs:
                ix.expansion_search = p["ef"]
            R = p["R"]

            def fn(i):
                ks, ds = [], []
                for ix in idxs:
                    m = ix.search(q[i], R, threads=1, exact=False)
                    ks.append(m.keys)
                    ds.append(m.distances)
                ks = np.concatenate(ks).astype(np.int64)
                ds = np.concatenate(ds)
                top = ks[np.argsort(ds)[:R]]
                return rescore(base, q[i], top)
            return fn
        rows += sweep(out, "usearch", f"i8+ricalcolo, {pieces} pezzi", pts, make, range(len(q)), gt, False,
                      extra={"build_s": bt, "pieces": pieces})
    return rows


# ---------------- faiss ----------------

def build_faiss(base, threads=2):
    import faiss
    faiss.omp_set_num_threads(threads)
    d = base.shape[1]
    sub = faiss.IndexHNSWSQ(d, faiss.ScalarQuantizer.QT_8bit, 16, faiss.METRIC_INNER_PRODUCT)
    sub.hnsw.efConstruction = 100
    t0 = time.time()
    train = np.asarray(base[np.random.default_rng(1).choice(base.shape[0], min(100_000, base.shape[0]), replace=False)])
    sub.train(train)
    idx = faiss.IndexRefineFlat(sub)
    step = 50_000
    for s in range(0, base.shape[0], step):
        idx.add(np.ascontiguousarray(base[s:s + step]))
    return idx, sub, time.time() - t0


def run_faiss(out, base, q, gt, gtf, cat, do_filter):
    import faiss
    idx, sub, bt = build_faiss(base)
    print(f"faiss HNSW SQ8 + refine costruito in {bt:.0f}s", flush=True)
    faiss.omp_set_num_threads(1)
    rows = []
    sel = None
    if do_filter:
        mask = (cat == FILTER_CAT)
        bitmap = np.packbits(mask, bitorder="little")
        sel = faiss.IDSelectorBitmap(len(mask), faiss.swig_ptr(bitmap))
    cases = [(False, gt)] + ([(True, gtf)] if do_filter else [])
    for filt, g in cases:
        for kf in (1, 2, 4, 8):
            efs = [16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024]
            pts = [{"ef": ef, "k_factor": kf} for ef in efs if ef >= 10 * kf or ef >= 64]

            def make(p):
                hp = faiss.SearchParametersHNSW(efSearch=p["ef"], sel=sel if filt else None)
                rp = faiss.IndexRefineSearchParameters(k_factor=p["k_factor"], base_index_params=hp)

                def fn(i):
                    _, I = idx.search(q[i:i + 1], 10, params=rp)
                    return I[0].tolist()
                return fn
            rows += sweep(out, "faiss", "HNSW SQ8+ricalcolo, 1 pezzo", pts, make, range(len(q)), g, filt,
                          extra={"build_s": bt, "pieces": 1})
    del idx
    return rows


def main():
    dd, out = sys.argv[1], sys.argv[2]
    which = sys.argv[3:] or ["usearch_i8", "usearch_b1", "faiss_sq8", "usearch_i8_8pezzi"]
    meta, base, q, gt, gtf, cat = load(dd)
    for w in which:
        t0 = time.time()
        if w == "usearch_i8":
            run_usearch(out, base, q, gt, "i8", save=os.environ.get("USEARCH_SAVE"))
        elif w == "usearch_b1":
            run_usearch(out, base, q, gt, "b1")
        elif w == "usearch_i8_8pezzi":
            run_usearch_pieces(out, base, q, gt)
        elif w == "faiss_sq8":
            run_faiss(out, base, q, gt, gtf, cat, do_filter=True)
        print(f"== {w} finito in {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
