"""M23: la nostra ricerca per significato contro i motori specializzati, sulla stessa macchina.

Uso: python3 specialisti.py <cartella dati> <risultati .jsonl> <job> <quali...>
quali:
  noi              faiss HNSW SQ8 + ricalcolo (la migliore di M20), con e senza filtro
  noi_usearch      usearch int8 + ricalcolo e binario + ricalcolo (M20)
  faiss_hnsw       FAISS HNSW32,Flat
  faiss_ivfpq      FAISS IVF4096,PQ192x4fs,RFlat
  hnswlib          hnswlib M16 efC200
  usearch_serie    usearch f16 di serie
  lancedb          LanceDB IVF_PQ (parametri predefiniti), coseno
  qdrant_sq        Qdrant HNSW + int8 (server già acceso: QDRANT_PID, QDRANT_PORT)
  qdrant_bq        Qdrant HNSW + binaria 1 bit
  qdrant_bq2       Qdrant HNSW + binaria 2 bit
  pgvector         PostgreSQL + pgvector HNSW (server già acceso: PG_DSN)

Una domanda alla volta. Ricerche per vCPU = domande / secondi di CPU spesi per cercare:
- librerie nel processo: time.process_time (tutti i thread del processo);
- server: utime+stime del processo server da /proc/<pid>/stat (tutti i thread).
Per i server (e LanceDB, che passa da un costruttore di domande in Python) misuro anche una domanda "vuota"
(stesso percorso, nessun lavoro di ricerca) e riporto qps_cpu_net = senza quel costo.
"""
import json, os, sys, time, subprocess
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "cuochi-vettori"))
from nostri import load, recall10, rescore, run_faiss, run_usearch  # noqa: E402  (codice di M20)
import nostri  # noqa: E402

FILTER_CAT = 3
JOB = "?"
CLK = os.sysconf("SC_CLK_TCK")


# ---------------- misura ----------------

def proc_meter_stat(pid):
    """utime+stime di tutto il processo (tutti i thread, anche quelli finiti), risoluzione 10 ms."""
    path = f"/proc/{pid}/stat"

    def m():
        with open(path) as f:
            s = f.read()
        parts = s[s.rindex(")") + 2:].split()
        return (int(parts[11]) + int(parts[12])) / CLK  # utime, stime
    return m


def proc_meter(pid):
    """Somma del tempo di CPU dei thread vivi (schedstat, in nanosecondi): molto più preciso di /proc/<pid>/stat.
    Controllato contro /proc/<pid>/stat con check_meter (i thread che finiscono durante la misura non si vedrebbero)."""
    base = f"/proc/{pid}/task"

    def m():
        tot = 0
        for t in os.listdir(base):
            try:
                with open(f"{base}/{t}/schedstat") as f:
                    tot += int(f.read().split()[0])
            except (FileNotFoundError, ProcessLookupError):
                pass
        return tot / 1e9
    return m


def check_meter(pid, fn, n):
    """Stesso giro misurato con i due contatori: devono coincidere (entro la risoluzione di 10 ms)."""
    a, b = proc_meter(pid), proc_meter_stat(pid)
    a0, b0 = a(), b()
    for i in range(n):
        fn(i)
    return {"schedstat_s": a() - a0, "stat_s": b() - b0, "n": n}


def timed(fn, idxs, meter):
    c0, w0 = meter(), time.perf_counter()
    res = [fn(i) for i in idxs]
    return res, meter() - c0, time.perf_counter() - w0


def overhead(fn_empty, idxs, meter, reps=2):
    """Secondi di CPU per domanda del percorso vuoto (protocollo, costruttore). Il minimo di reps giri."""
    timed(fn_empty, idxs, meter)
    best = None
    for _ in range(reps):
        _, cpu, _ = timed(fn_empty, idxs, meter)
        best = cpu if best is None else min(best, cpu)
    return best / len(idxs)


def sweep(out, engine, config, points, make_fn, nq, gt, filt, meter, ovh=0.0, extra=None, stop_at=0.975):
    """Come M20: riscaldamento, una passata per punto (si ferma a richiamo stop_at), poi i punti decisivi
    (più veloce sopra 0,90 e 0,95) rimisurati 2 volte, si tiene il migliore."""
    idxs = range(nq)
    timed(make_fn(points[0]), idxs, meter)  # riscaldamento
    rows = []

    def fill(row, cpu, wall):
        row["cpu_s"] = cpu
        row["qps_cpu"] = nq / cpu
        row["qps_wall"] = nq / wall
        if ovh:
            net = max(cpu - ovh * nq, 1e-9)
            row["qps_cpu_net"] = nq / net
            row["ovh_ms"] = ovh * 1e3
        else:
            row["qps_cpu_net"] = row["qps_cpu"]

    for p in points:
        res, cpu, wall = timed(make_fn(p), idxs, meter)
        r = recall10(res, gt[:nq])
        row = {"job": JOB, "engine": engine, "config": config, "filter": filt, "params": p, "recall10": r, "nq": nq}
        if extra:
            row.update(extra)
        fill(row, cpu, wall)
        rows.append(row)
        print(json.dumps(row), flush=True)
        if r >= stop_at:
            break
    for target in (0.90, 0.95):
        ok = [x for x in rows if x["recall10"] >= target]
        if not ok:
            continue
        best = max(ok, key=lambda x: x["qps_cpu"])
        fn = make_fn(best["params"])
        for _ in range(2):
            _, cpu, wall = timed(fn, idxs, meter)
            if nq / cpu > best["qps_cpu"]:
                fill(best, cpu, wall)
        best["confirmed"] = True
    with open(out, "a") as f:
        for x in rows:
            f.write(json.dumps(x) + "\n")
    return rows


def log_build(out, engine, config, **kw):
    row = {"job": JOB, "engine": engine, "config": config, "kind": "build", **kw}
    print(json.dumps(row), flush=True)
    with open(out.replace(".jsonl", "-build.jsonl"), "a") as f:
        f.write(json.dumps(row) + "\n")


# ---------------- noi (M20) ----------------

def run_noi(out, base, q, gt, gtf, cat):
    # nostri.sweep misura thread_time; qui misuro anche process_time per stare sulla stessa misura degli avversari
    rows = []
    orig_timed = nostri.timed

    def both(fn, queries):
        c0, p0, w0 = time.thread_time(), time.process_time(), time.perf_counter()
        res = [fn(x) for x in queries]
        cpu_t, cpu_p = time.thread_time() - c0, time.process_time() - p0
        return res, cpu_p, time.perf_counter() - w0
    nostri.timed = both
    tmp = out + ".noi.tmp"
    try:
        run_faiss(tmp, base, q, gt, gtf, cat, do_filter=True)
    finally:
        nostri.timed = orig_timed
    with open(out, "a") as f:
        for l in open(tmp):
            r = json.loads(l)
            r["job"] = JOB
            r["engine"] = "noi (faiss)"
            r["qps_cpu_net"] = r["qps_cpu"]
            f.write(json.dumps(r) + "\n")
            rows.append(r)
    os.remove(tmp)
    return rows


def run_noi_usearch(out, base, q, gt):
    tmp = out + ".nu.tmp"
    orig = nostri.timed

    def proc(fn, queries):
        p0, w0 = time.process_time(), time.perf_counter()
        res = [fn(x) for x in queries]
        return res, time.process_time() - p0, time.perf_counter() - w0
    nostri.timed = proc
    try:
        run_usearch(tmp, base, q, gt, "i8", save=os.environ.get("USEARCH_SAVE"))
        run_usearch(tmp, base, q, gt, "b1")
    finally:
        nostri.timed = orig
    with open(out, "a") as f:
        for l in open(tmp):
            r = json.loads(l)
            r["job"] = JOB
            r["engine"] = "noi (usearch)"
            r["qps_cpu_net"] = r["qps_cpu"]
            f.write(json.dumps(r) + "\n")
    os.remove(tmp)


# ---------------- FAISS ----------------

def faiss_selector(cat):
    import faiss
    mask = (cat == FILTER_CAT)
    bitmap = np.packbits(mask, bitorder="little")
    sel = faiss.IDSelectorBitmap(len(mask), faiss.swig_ptr(bitmap))
    return sel, bitmap  # bitmap va tenuto vivo


def add_batches(idx, base, step=50_000):
    for s in range(0, base.shape[0], step):
        idx.add(np.ascontiguousarray(base[s:s + step]))


def run_faiss_hnsw(out, base, q, gt, gtf, cat):
    import faiss
    faiss.omp_set_num_threads(2)
    d = base.shape[1]
    idx = faiss.IndexHNSWFlat(d, 32, faiss.METRIC_INNER_PRODUCT)
    idx.hnsw.efConstruction = 100
    t0 = time.time()
    add_batches(idx, base)
    bt = time.time() - t0
    log_build(out, "faiss", "HNSW32,Flat", build_s=bt)
    faiss.omp_set_num_threads(1)
    sel, _keep = faiss_selector(cat)
    for filt, g in ((False, gt), (True, gtf)):
        efs = [10, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024]

        def make(p, filt=filt):
            sp = faiss.SearchParametersHNSW(efSearch=p["ef"], sel=sel if filt else None)

            def fn(i):
                _, I = idx.search(q[i:i + 1], 10, params=sp)
                return I[0].tolist()
            return fn
        sweep(out, "faiss", "HNSW32,Flat", [{"ef": e} for e in efs], make, len(q), g, filt, time.process_time,
              extra={"build_s": bt, "pieces": 1})
    del idx


def run_faiss_ivfpq(out, base, q, gt, gtf, cat):
    import faiss
    faiss.omp_set_num_threads(2)
    d = base.shape[1]
    idx = faiss.index_factory(d, "IVF4096,PQ192x4fs,RFlat", faiss.METRIC_INNER_PRODUCT)
    t0 = time.time()
    train = np.asarray(base[np.sort(np.random.default_rng(2).choice(base.shape[0], min(200_000, base.shape[0]), replace=False))])
    idx.train(train)
    del train
    add_batches(idx, base)
    bt = time.time() - t0
    log_build(out, "faiss", "IVF4096,PQ192x4fs,RFlat", build_s=bt)
    faiss.omp_set_num_threads(1)
    sel, _keep = faiss_selector(cat)
    for filt, g in ((False, gt), (True, gtf)):
        for kf in (4, 8, 16, 32):
            pts = [{"nprobe": n, "k_factor": kf} for n in (8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256)]

            def make(p, filt=filt):
                ip = faiss.SearchParametersIVF(nprobe=p["nprobe"], sel=sel if filt else None)
                rp = faiss.IndexRefineSearchParameters(k_factor=p["k_factor"], base_index_params=ip)

                def fn(i):
                    _, I = idx.search(q[i:i + 1], 10, params=rp)
                    return I[0].tolist()
                return fn
            sweep(out, "faiss", "IVF4096,PQ192x4fs,RFlat", pts, make, len(q), g, filt, time.process_time,
                  extra={"build_s": bt, "pieces": 1})
    del idx


# ---------------- hnswlib, usearch ----------------

def run_hnswlib(out, base, q, gt):
    import hnswlib
    n, d = base.shape
    idx = hnswlib.Index(space="ip", dim=d)
    idx.init_index(max_elements=n, ef_construction=200, M=16)
    t0 = time.time()
    step = 50_000
    for s in range(0, n, step):
        idx.add_items(np.ascontiguousarray(base[s:s + step]), np.arange(s, min(s + step, n)), num_threads=2)
    bt = time.time() - t0
    log_build(out, "hnswlib", "M16 efC200 float32", build_s=bt)
    idx.set_num_threads(1)
    efs = [10, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024]

    def make(p):
        idx.set_ef(p["ef"])

        def fn(i):
            labels, _ = idx.knn_query(q[i], k=10, num_threads=1)
            return labels[0].tolist()
        return fn
    sweep(out, "hnswlib", "M16 efC200 float32", [{"ef": e} for e in efs], make, len(q), gt, False,
          time.process_time, extra={"build_s": bt, "pieces": 1})
    del idx


def run_usearch_serie(out, base, q, gt):
    from usearch.index import Index
    n, d = base.shape
    idx = Index(ndim=d, metric="ip", dtype="f16")  # predefiniti: connectivity 16, expansion_add 128
    t0 = time.time()
    step = 50_000
    for s in range(0, n, step):
        idx.add(np.arange(s, min(s + step, n), dtype=np.uint64), np.ascontiguousarray(base[s:s + step]), threads=2)
    bt = time.time() - t0
    log_build(out, "usearch", "f16 di serie", build_s=bt, connectivity=idx.connectivity,
              expansion_add=idx.expansion_add, hw=str(idx.hardware_acceleration))
    efs = [10, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024]

    def make(p):
        idx.expansion_search = p["ef"]

        def fn(i):
            m = idx.search(q[i], 10, threads=1, exact=False)
            return m.keys.astype(np.int64).tolist()
        return fn
    sweep(out, "usearch", "f16 di serie", [{"ef": e} for e in efs], make, len(q), gt, False, time.process_time,
          extra={"build_s": bt, "pieces": 1})
    del idx


# ---------------- LanceDB ----------------

def run_lancedb(out, base, q, gt, gtf, cat, path):
    os.environ.setdefault("RUST_LOG", "error")  # niente avvisi a ogni domanda (costano CPU)
    os.environ.setdefault("LANCE_LOG", "error")
    import warnings
    warnings.filterwarnings("ignore")
    import lancedb
    import pyarrow as pa
    n, d = base.shape
    db = lancedb.connect(path)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("cat", pa.int32()),
                        pa.field("vector", pa.list_(pa.float32(), d))])

    def batches(step=100_000):
        for s in range(0, n, step):
            e = min(s + step, n)
            x = np.ascontiguousarray(base[s:e]).reshape(-1)
            vec = pa.FixedSizeListArray.from_arrays(pa.array(x, type=pa.float32()), d)
            yield pa.record_batch([pa.array(np.arange(s, e, dtype=np.int64)), pa.array(cat[s:e].astype(np.int32)), vec],
                                  schema=schema)
    t0 = time.time()
    tbl = db.create_table("cohere", data=pa.RecordBatchReader.from_batches(schema, batches()), mode="overwrite")
    t_load = time.time() - t0
    tbl.create_index(metric="cosine", vector_column_name="vector", index_type="IVF_PQ")
    tbl.create_scalar_index("cat", index_type="BITMAP")
    bt = time.time() - t0
    try:
        stats = tbl.index_stats("vector_idx")
        stats = str(stats)
    except Exception as e:  # noqa
        stats = repr(e)
    log_build(out, "lancedb", "IVF_PQ predefinito", build_s=bt, load_s=t_load, index=stats,
              version=lancedb.__version__)

    def query(i, nprobes, refine, where):
        b = tbl.search(q[i], vector_column_name="vector").distance_type("cosine").nprobes(nprobes)
        if hasattr(b, "disable_scoring_autoprojection"):
            b = b.disable_scoring_autoprojection()
        if refine:
            b = b.refine_factor(refine)
        if where:
            b = b.where(where, prefilter=True)
        return b.select(["id", "_distance"]).limit(10).to_arrow().column("id").to_pylist()

    empty = lambda i: query(i, 1, None, "cat = 99")
    ovh = overhead(empty, range(len(q)), time.process_time)
    log_build(out, "lancedb", "domanda vuota", ovh_ms=ovh * 1e3)
    for filt, g in ((False, gt), (True, gtf)):
        for refine in (None, 10, 20, 40):
            pts = [{"nprobes": p, "refine": refine} for p in (5, 10, 15, 20, 30, 40, 60, 80, 120, 160, 240)]

            def make(p, filt=filt):
                return lambda i: query(i, p["nprobes"], p["refine"], "cat = 3" if filt else None)
            sweep(out, "lancedb", "IVF_PQ predefinito", pts, make, len(q), g, filt, time.process_time, ovh=ovh,
                  extra={"build_s": bt, "pieces": 1})


# ---------------- Qdrant ----------------

def qdrant_segments(name):
    """Punti per segmento (telemetria di Qdrant), per controllare che l'indice sia in un pezzo."""
    import urllib.request
    try:
        t = json.load(urllib.request.urlopen("http://127.0.0.1:6333/telemetry?details_level=3", timeout=30))
        out = []
        for c in t["result"]["collections"]["collections"]:
            if c.get("id") != name:
                continue
            for sh in c.get("shards", []):
                for sg in (sh.get("local") or {}).get("segments", []):
                    i = sg.get("info", {})
                    out.append({"tipo": i.get("segment_type"), "punti": i.get("num_points"),
                                "indicizzati": i.get("num_indexed_vectors")})
        return out
    except Exception as e:  # noqa
        return repr(e)


def run_qdrant(out, base, q, gt, gtf, cat, kind):
    from qdrant_client import QdrantClient, models as m
    pid = int(os.environ["QDRANT_PID"])
    meter = proc_meter(pid)
    cl = QdrantClient(host="127.0.0.1", grpc_port=int(os.environ.get("QDRANT_GRPC", 6334)), prefer_grpc=True,
                      timeout=600)
    n, d = base.shape
    name = f"cohere_{kind}"
    if kind == "sq":
        quant = m.ScalarQuantization(scalar=m.ScalarQuantizationConfig(type=m.ScalarType.INT8, quantile=0.99,
                                                                       always_ram=True))
        oss = (1.0, 2.0, 3.0)
        label = "HNSW + int8 (scalare)"
    else:
        enc = m.BinaryQuantizationEncoding.ONE_BIT if kind == "bq" else m.BinaryQuantizationEncoding.TWO_BITS
        quant = m.BinaryQuantization(binary=m.BinaryQuantizationConfig(always_ram=True, encoding=enc))
        oss = (4.0, 8.0, 16.0, 32.0) if kind == "bq" else (2.0, 4.0, 8.0, 16.0)
        label = "HNSW + binaria 1 bit" if kind == "bq" else "HNSW + binaria 2 bit"
    if cl.collection_exists(name):
        cl.delete_collection(name)
    cl.create_collection(
        name,
        vectors_config=m.VectorParams(size=d, distance=m.Distance.COSINE, on_disk=True),
        hnsw_config=m.HnswConfigDiff(m=16, ef_construct=100),
        optimizers_config=m.OptimizersConfigDiff(default_segment_number=1, max_segment_size=50_000_000,
                                                 indexing_threshold=0),
        quantization_config=quant,
    )
    t0 = time.time()
    step = 10_000
    for s in range(0, n, step):
        e = min(s + step, n)
        cl.upload_collection(name, vectors=np.ascontiguousarray(base[s:e]),
                             payload=[{"cat": int(c)} for c in cat[s:e]], ids=list(range(s, e)),
                             batch_size=500, parallel=1, wait=False)
    cl.create_payload_index(name, "cat", field_schema=m.PayloadSchemaType.INTEGER, wait=True)
    t_load = time.time() - t0
    cl.update_collection(name, optimizers_config=m.OptimizersConfigDiff(indexing_threshold=20_000))
    while True:
        time.sleep(10)
        info = cl.get_collection(name)
        ok = (info.status == m.CollectionStatus.GREEN and (info.indexed_vectors_count or 0) >= n
              and (info.points_count or 0) >= n)
        if ok:
            break
        print(f"qdrant {kind}: stato {info.status} indicizzati {info.indexed_vectors_count} punti {info.points_count} "
              f"segmenti {info.segments_count} {time.time()-t0:.0f}s", flush=True)
    bt = time.time() - t0
    info = cl.get_collection(name)
    try:
        ver = cl.info().version
    except Exception:  # noqa
        ver = "?"
    log_build(out, "qdrant", label, build_s=bt, load_s=t_load, segments=info.segments_count, version=ver,
              points=info.points_count, indexed=info.indexed_vectors_count)
    filt3 = m.Filter(must=[m.FieldCondition(key="cat", match=m.MatchValue(value=FILTER_CAT))])
    filt99 = m.Filter(must=[m.FieldCondition(key="cat", match=m.MatchValue(value=99))])
    ql = [x.tolist() for x in q]

    def query(i, ef, os_, flt):
        r = cl.query_points(name, query=ql[i], limit=10, with_payload=False, query_filter=flt,
                            search_params=m.SearchParams(hnsw_ef=ef, quantization=m.QuantizationSearchParams(
                                rescore=True, oversampling=os_)))
        return [p.id for p in r.points]

    ovh = overhead(lambda i: query(i, 16, 1.0, filt99), range(len(q)), meter)
    chk = check_meter(pid, lambda i: query(i, 64, 2.0, None), len(q))
    log_build(out, "qdrant", label + " domanda vuota", ovh_ms=ovh * 1e3, controllo_contatori=chk,
              segmenti=qdrant_segments(name))
    efs = [16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512]
    for filt, g in ((False, gt), (True, gtf)):
        for os_ in oss:
            pts = [{"hnsw_ef": e, "oversampling": os_} for e in efs]

            def make(p, filt=filt):
                return lambda i: query(i, p["hnsw_ef"], p["oversampling"], filt3 if filt else None)
            sweep(out, "qdrant", label, pts, make, len(q), g, filt, meter, ovh=ovh,
                  extra={"build_s": bt, "pieces": info.segments_count, "version": ver})
    cl.delete_collection(name)


# ---------------- pgvector ----------------

def pg_copy_binary(conn, base, cat, step=20_000):
    """COPY binario costruito con numpy: (id int8, cat int4, embedding vector)."""
    n, d = base.shape
    dt = np.dtype([("nf", ">i2"), ("l1", ">i4"), ("id", ">i8"), ("l2", ">i4"), ("cat", ">i4"), ("l3", ">i4"),
                   ("dim", ">i2"), ("unused", ">i2"), ("v", ">f4", (d,))])
    with conn.cursor() as cur:
        with cur.copy("COPY items (id, cat, embedding) FROM STDIN WITH (FORMAT BINARY)") as cp:
            cp.write(b"PGCOPY\n\xff\r\n\x00" + (0).to_bytes(4, "big") + (0).to_bytes(4, "big"))
            for s in range(0, n, step):
                e = min(s + step, n)
                a = np.empty(e - s, dtype=dt)
                a["nf"], a["l1"], a["id"], a["l2"], a["cat"] = 3, 8, np.arange(s, e), 4, cat[s:e]
                a["l3"], a["dim"], a["unused"] = 4 + 4 * d, d, 0
                a["v"] = base[s:e]
                cp.write(a.tobytes())
            cp.write((-1).to_bytes(2, "big", signed=True))
    conn.commit()


def run_pgvector(out, base, q, gt, gtf, cat):
    import psycopg
    dsn = os.environ.get("PG_DSN", "host=/tmp port=5433 dbname=postgres")
    conn = psycopg.connect(dsn)
    n, d = base.shape
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
        ver = cur.fetchone()[0]
        cur.execute("DROP TABLE IF EXISTS items")
        cur.execute(f"CREATE TABLE items (id bigint PRIMARY KEY, cat int NOT NULL, embedding vector({d}) NOT NULL)")
        cur.execute("ALTER TABLE items ALTER COLUMN embedding SET STORAGE PLAIN")
    conn.commit()
    t0 = time.time()
    pg_copy_binary(conn, base, cat)
    t_load = time.time() - t0
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX items_hnsw ON items USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)")
        cur.execute("VACUUM ANALYZE items")
        cur.execute("SELECT pg_size_pretty(pg_relation_size('items_hnsw')), pg_size_pretty(pg_relation_size('items'))")
        sizes = cur.fetchone()
    bt = time.time() - t0
    log_build(out, "pgvector", "HNSW m16 efC64", build_s=bt, load_s=t_load, version=ver, sizes=list(sizes))
    pid = conn.info.backend_pid
    meter = proc_meter(pid)
    ql = ["[" + ",".join(f"{v:.7g}" for v in x) + "]" for x in q]
    cur = conn.cursor()
    cur.execute("SET jit = off")
    has_iter = tuple(int(x) for x in ver.split(".")[:2]) >= (0, 8)
    if has_iter:
        cur.execute("SET hnsw.iterative_scan = relaxed_order")
    # controllo del piano
    cur.execute("EXPLAIN SELECT id FROM items ORDER BY embedding <=> %s::vector LIMIT 10", (ql[0],))
    import re
    cut = lambda t: re.sub(r"'\[[^\]]*\]'", "'[...]'", t)
    plan = cut(" | ".join(r[0] for r in cur.fetchall()))
    cur.execute("EXPLAIN SELECT id FROM items WHERE cat = 3 ORDER BY embedding <=> %s::vector LIMIT 10", (ql[0],))
    planf = cut(" | ".join(r[0] for r in cur.fetchall()))
    log_build(out, "pgvector", "piani", plan=plan, plan_filter=planf, iterative=has_iter)
    sql = "SELECT id FROM items ORDER BY embedding <=> %s::vector LIMIT 10"
    sqlf = "SELECT id FROM items WHERE cat = 3 ORDER BY embedding <=> %s::vector LIMIT 10"

    def empty(i):
        cur.execute("SELECT %s::vector IS NULL", (ql[i],), prepare=True)
        return cur.fetchall()
    ovh = overhead(empty, range(len(q)), meter)
    cur.execute("SET hnsw.ef_search = 64")
    chk = check_meter(pid, lambda i: (cur.execute(sql, (ql[i],), prepare=True), cur.fetchall()), len(q))
    log_build(out, "pgvector", "domanda vuota", ovh_ms=ovh * 1e3, controllo_contatori=chk)
    efs = [10, 16, 24, 32, 40, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1000]
    for filt, g in ((False, gt), (True, gtf)):
        if filt and not has_iter:
            continue

        def make(p, filt=filt):
            cur.execute(f"SET hnsw.ef_search = {p['ef_search']}")
            s = sqlf if filt else sql

            def fn(i):
                cur.execute(s, (ql[i],), prepare=True)
                return [r[0] for r in cur.fetchall()]
            return fn
        sweep(out, "pgvector", "HNSW m16 efC64", [{"ef_search": e} for e in efs], make, len(q), g, filt, meter,
              ovh=ovh, extra={"build_s": bt, "pieces": 1, "version": ver})
    cur.execute("DROP TABLE items")
    conn.close()


def main():
    global JOB
    dd, out, JOB = sys.argv[1], sys.argv[2], sys.argv[3]
    which = sys.argv[4:]
    meta, base, q, gt, gtf, cat = load(dd)
    nq = int(os.environ.get("NQ", len(q)))
    q, gt, gtf = q[:nq], gt[:nq], gtf[:nq]
    for w in which:
        t0 = time.time()
        print(f"== {w} inizio", flush=True)
        if w == "noi":
            run_noi(out, base, q, gt, gtf, cat)
        elif w == "noi_usearch":
            run_noi_usearch(out, base, q, gt)
        elif w == "faiss_hnsw":
            run_faiss_hnsw(out, base, q, gt, gtf, cat)
        elif w == "faiss_ivfpq":
            run_faiss_ivfpq(out, base, q, gt, gtf, cat)
        elif w == "hnswlib":
            run_hnswlib(out, base, q, gt)
        elif w == "usearch_serie":
            run_usearch_serie(out, base, q, gt)
        elif w == "lancedb":
            run_lancedb(out, base, q, gt, gtf, cat, os.environ.get("LANCE_PATH", "/tmp/lance"))
        elif w in ("qdrant_sq", "qdrant_bq", "qdrant_bq2"):
            run_qdrant(out, base, q, gt, gtf, cat, w.split("_")[1])
        elif w == "pgvector":
            run_pgvector(out, base, q, gt, gtf, cat)
        else:
            raise SystemExit(f"sconosciuto: {w}")
        print(f"== {w} finito in {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
