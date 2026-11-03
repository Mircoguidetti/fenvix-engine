"""M20: prepara i dati per le misure.

Legge i parquet di VectorDBBench Cohere 1M (o il ripiego Hugging Face), scrive file grezzi:
  base.f32      N x D float32, normalizzati
  queries.f32   Q x D float32, normalizzati
  cat.u8        N byte, campo del filtro (0..9, pseudocasuale dall'indice)
  gt.i32        Q x 100 vicini veri (forza bruta, prodotto scalare)
  gtf.i32       Q x 100 vicini veri con filtro cat == 3
  seq_<c>.f32 / seq_<c>.json / seq_<c>_gt.i32   sequenze "agenti" (leva 3)
  meta.json
Uso: python3 dati.py <cartella parquet> <cartella uscita> [--max N]
"""
import json, os, sys, time
import numpy as np
import pyarrow.parquet as pq

D_EXPECTED = 768
FILTER_CAT = 3


def cat_of(n):
    i = np.arange(n, dtype=np.uint64)
    h = (i * np.uint64(2654435761)) % np.uint64(2**32)
    h = (h ^ (h >> np.uint64(13))) * np.uint64(0x5bd1e995) % np.uint64(2**32)
    return (h % np.uint64(10)).astype(np.uint8)


def vec_column(schema_names):
    for c in ("emb", "embedding", "emb_768", "vector", "embeddings"):
        if c in schema_names:
            return c
    raise SystemExit(f"colonna vettori non trovata: {schema_names}")


def write_parquet_vectors(files, out_path, max_rows, skip_rows=0):
    """Scrive i vettori normalizzati da una lista di parquet. Ritorna (righe, dim)."""
    n = 0
    seen = 0
    dim = None
    with open(out_path, "wb") as f:
        for path in files:
            pf = pq.ParquetFile(path)
            col = vec_column(pf.schema_arrow.names)
            for batch in pf.iter_batches(batch_size=20000, columns=[col]):
                arr = batch.column(0)
                flat = arr.flatten().to_numpy(zero_copy_only=False).astype(np.float32)
                m = len(arr)
                dim = flat.size // m
                x = flat.reshape(m, dim)
                if seen + m <= skip_rows:
                    seen += m
                    continue
                if seen < skip_rows:
                    x = x[skip_rows - seen:]
                    seen = skip_rows
                seen += len(x)
                if n + len(x) > max_rows:
                    x = x[: max_rows - n]
                x /= np.linalg.norm(x, axis=1, keepdims=True) + 1e-12
                f.write(np.ascontiguousarray(x, dtype=np.float32).tobytes())
                n += len(x)
                if n >= max_rows:
                    return n, dim
    return n, dim


def brute_topk(base, queries, k=100, mask=None, block=100_000):
    """Vicini veri per prodotto scalare, a blocchi. mask: bool N (True = ammesso)."""
    q = queries.shape[0]
    best_s = np.full((q, k), -np.inf, dtype=np.float32)
    best_i = np.full((q, k), -1, dtype=np.int64)
    n = base.shape[0]
    for s in range(0, n, block):
        b = np.asarray(base[s:s + block])
        sc = queries @ b.T
        if mask is not None:
            sc[:, ~mask[s:s + block]] = -np.inf
        kk = min(k, sc.shape[1])
        idx = np.argpartition(-sc, kk - 1, axis=1)[:, :kk]
        ss = np.take_along_axis(sc, idx, axis=1)
        cs = np.concatenate([best_s, ss], axis=1)
        ci = np.concatenate([best_i, idx + s], axis=1)
        top = np.argpartition(-cs, k - 1, axis=1)[:, :k]
        best_s = np.take_along_axis(cs, top, axis=1)
        best_i = np.take_along_axis(ci, top, axis=1)
    order = np.argsort(-best_s, axis=1)
    return np.take_along_axis(best_i, order, axis=1).astype(np.int32)


def gt_chunks(base, queries, mask=None, chunk=500):
    out = []
    for s in range(0, len(queries), chunk):
        out.append(brute_topk(base, queries[s:s + chunk], mask=mask))
    return np.concatenate(out)


def make_sequence(queries, cos, rng, n_tasks=200, attempts=5, per_attempt=3):
    """Sequenza 'agenti' (modello STIMATO, caso paralleli di M18).
    Ogni compito: 5 tentativi in parallelo; ogni tentativo fa 3 domande a ~4 s.
    Domanda 1 = base del compito (ripetizione esatta tra tentativi), 2 e 3 = parafrasi
    (base + rumore isotropo con coseno atteso `cos`)."""
    d = queries.shape[1]
    sigma = np.sqrt(1.0 / cos**2 - 1.0)
    vecs, info = [], []
    for t in range(n_tasks):
        base = queries[t]
        t0 = 30.0 * t
        for a in range(attempts):
            ta = t0 + rng.uniform(0, 2)
            for j in range(per_attempt):
                if j == 0:
                    v = base.copy()
                    kind = "esatta"
                else:
                    noise = rng.standard_normal(d).astype(np.float32) / np.sqrt(d)
                    v = base + sigma * noise
                    v /= np.linalg.norm(v)
                    kind = "parafrasi"
                vecs.append(v.astype(np.float32))
                info.append({"t": ta + 4.0 * j + rng.uniform(0, 1), "task": t, "attempt": a,
                             "kind": kind, "cos_base": float(v @ base)})
    order = np.argsort([x["t"] for x in info], kind="stable")
    vecs = np.stack([vecs[i] for i in order])
    info = [info[i] for i in order]
    return vecs, info


def main():
    src, out = sys.argv[1], sys.argv[2]
    max_n = 1_000_000
    if "--max" in sys.argv:
        max_n = int(sys.argv[sys.argv.index("--max") + 1])
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    meta = {"source": src}
    files = sorted(os.listdir(src))
    train = [os.path.join(src, f) for f in files if f.startswith(("train", "shuffle_train"))]
    test = [os.path.join(src, f) for f in files if f.startswith("test")]
    hf = [os.path.join(src, f) for f in files if f.startswith("hf-")]
    if train and test:
        meta["dataset"] = "VectorDBBench cohere_medium_1m"
        n, dim = write_parquet_vectors(train, f"{out}/base.f32", max_n)
        nq, _ = write_parquet_vectors(test, f"{out}/queries.f32", 1000)
    elif hf:
        meta["dataset"] = "Cohere/wikipedia-22-12-en-embeddings (prime righe)"
        n, dim = write_parquet_vectors(hf, f"{out}/base.f32", max_n)
        nq, _ = write_parquet_vectors(hf, f"{out}/queries.f32", 1000, skip_rows=n)
    else:
        raise SystemExit(f"nessun parquet riconosciuto in {src}: {files}")
    meta.update({"n": n, "dim": dim, "nq": nq})
    print("vettori scritti", meta, f"{time.time()-t0:.0f}s", flush=True)

    base = np.memmap(f"{out}/base.f32", dtype=np.float32, mode="r", shape=(n, dim))
    queries = np.fromfile(f"{out}/queries.f32", dtype=np.float32).reshape(nq, dim)
    cat = cat_of(n)
    cat.tofile(f"{out}/cat.u8")
    mask = cat == FILTER_CAT
    meta["filter_pass"] = float(mask.mean())

    gt = gt_chunks(base, queries)
    gt.tofile(f"{out}/gt.i32")
    print("gt", f"{time.time()-t0:.0f}s", flush=True)
    gtf = gt_chunks(base, queries, mask=mask)
    gtf.tofile(f"{out}/gtf.i32")
    print("gt filtro", f"{time.time()-t0:.0f}s", flush=True)

    # controllo con le risposte dell'insieme, se ci sono
    nb = [os.path.join(src, f) for f in files if f == "neighbors.parquet"]
    if nb:
        try:
            t = pq.read_table(nb[0])
            col = [c for c in t.column_names if "neighbor" in c][0]
            theirs = t.column(col).to_pylist()
            agree = np.mean([len(set(theirs[i][:10]) & set(gt[i][:10].tolist())) / 10
                             for i in range(min(nq, len(theirs)))])
            meta["gt_agreement_top10_vs_dataset"] = float(agree)
        except Exception as e:  # noqa
            meta["gt_agreement_error"] = repr(e)

    rng = np.random.default_rng(20261103)
    for cos in (0.95, 0.90):
        tag = f"{int(cos*100):03d}"
        vecs, info = make_sequence(queries, cos, rng)
        vecs.tofile(f"{out}/seq_{tag}.f32")
        json.dump(info, open(f"{out}/seq_{tag}.json", "w"))
        gt_chunks(base, vecs).tofile(f"{out}/seq_{tag}_gt.i32")
        meta[f"seq_{tag}"] = len(vecs)
        print("sequenza", tag, len(vecs), f"{time.time()-t0:.0f}s", flush=True)
    meta["seconds"] = time.time() - t0
    json.dump(meta, open(f"{out}/meta.json", "w"), indent=1)
    print(json.dumps(meta), flush=True)


if __name__ == "__main__":
    main()
