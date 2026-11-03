"""Passi del workflow M24. Solo letture di dati pubblici, nessuna credenziale.

gh:   scarica le ore di GH Archive indicate in run.json e tiene solo (ora, actor, repo, tipo) per un
      campione di utenti (md5 del nome mod N == 0, come actor o come padrone del repository).
ecom: cerca copie pubbliche di tracce e-commerce con utente e ora (Hugging Face, sito REES46),
      e se run.json indica un file, lo scarica e tiene solo le colonne utili.
"""
import gzip, hashlib, io, json, os, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

try:
    import orjson as _j
    loads = _j.loads
except Exception:
    loads = json.loads

PARTE, OUT = sys.argv[1], sys.argv[2]
CFG = json.load(open("proto/riscaldamento-utente/ci/run.json"))
UA = {"User-Agent": "fenvix-m24-research/1.0"}
BYTES = [0]


def get(url, rng=None, timeout=120):
    h = dict(UA)
    if rng:
        h["Range"] = rng
    return urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout)


class Counter(io.RawIOBase):
    def __init__(self, f):
        self.f, self.n = f, 0

    def readable(self):
        return True

    def readinto(self, b):
        d = self.f.read(len(b))
        self.n += len(d)
        b[:len(d)] = d
        return len(d)


def sampled(login, mod):
    return int(hashlib.md5(login.lower().encode()).hexdigest()[:8], 16) % mod == 0


def gh_hour(args):
    day, h, mod = args
    url = f"https://data.gharchive.org/{day}-{h}.json.gz"
    rows, n = [], 0
    for attempt in range(3):
        try:
            r = get(url, timeout=300)
            c = Counter(r)
            g = gzip.GzipFile(fileobj=io.BufferedReader(c, 1 << 20))
            rows, n = [], 0
            for line in g:
                n += 1
                try:
                    e = loads(line)
                except Exception:
                    continue
                a = (e.get("actor") or {}).get("login") or ""
                rp = (e.get("repo") or {}).get("name") or ""
                own = rp.split("/", 1)[0]
                if not (sampled(a, mod) or sampled(own, mod)):
                    continue
                t = int(datetime.strptime(e["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
                rows.append(f"{t}\t{a}\t{rp}\t{e.get('type')}\n")
            return url, n, c.n, rows, None
        except Exception as ex:
            err = repr(ex)
            time.sleep(5)
    return url, 0, 0, [], err


def do_gh():
    mod = CFG.get("campione_mod", 10)
    start = datetime.strptime(CFG["gh_inizio"], "%Y-%m-%d")
    jobs = []
    for i in range(CFG["gh_ore"]):
        t = start + timedelta(hours=i)
        jobs.append((t.strftime("%Y-%m-%d"), t.hour, mod))
    tot_ev = tot_b = 0
    errs = []
    with gzip.open(f"{OUT}/gh-eventi.tsv.gz", "wt") as out, ThreadPoolExecutor(4) as ex:
        for url, n, b, rows, err in ex.map(gh_hour, jobs):
            tot_ev += n
            tot_b += b
            if err:
                errs.append((url, err))
            out.writelines(rows)
            print(url, n, b, len(rows), err or "", flush=True)
    json.dump({"ore": len(jobs), "eventi_letti": tot_ev, "byte_scaricati": tot_b, "errori": errs,
               "campione": f"md5(login) mod {mod} == 0"}, open(f"{OUT}/gh-conti.json", "w"), indent=1)


def do_ecom():
    found = {}
    for q in ["rees46", "retailrocket", "ecommerce behavior", "multi category store", "ecommerce events",
              "user behavior taobao", "ecommerce-behavior", "online retail"]:
        try:
            r = get("https://huggingface.co/api/datasets?search=" + urllib.parse.quote(q) + "&limit=40&full=false")
            found[q] = [d.get("id") for d in json.loads(r.read())]
        except Exception as ex:
            found[q] = repr(ex)
    trees = {}
    ids = sorted({i for v in found.values() if isinstance(v, list) for i in v})[:60]
    for i in ids:
        try:
            r = get(f"https://huggingface.co/api/datasets/{i}/tree/main?recursive=true")
            trees[i] = [(x.get("path"), x.get("size")) for x in json.loads(r.read()) if x.get("type") == "file"][:60]
        except Exception as ex:
            trees[i] = repr(ex)
    heads = {}
    for u in ["https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz",
              "https://data.rees46.com/datasets/marketplace/2019-Nov.csv.gz",
              "https://data.rees46.com/datasets/marketplace/2020-Apr.csv.gz"]:
        try:
            r = get(u, rng="bytes=0-65535", timeout=60)
            heads[u] = {"status": r.status, "len": r.headers.get("Content-Range")}
        except Exception as ex:
            heads[u] = repr(ex)
    json.dump({"ricerche": found, "file": trees, "rees46": heads}, open(f"{OUT}/esplora.json", "w"), indent=1)
    # scaricamento, se indicato in run.json
    src = CFG.get("ecom_url")
    if src:
        do_ecom_get(src, CFG.get("ecom_formato", "rees46"))


def do_ecom_get(urls, fmt):
    """Scarica e tiene: ora, utente, evento, oggetto. Tetto di byte in run.json (ecom_tetto_mb)."""
    import csv
    cap = CFG.get("ecom_tetto_mb", 3000) * 1_000_000
    if isinstance(urls, str):
        urls = [urls]
    tot = 0
    with gzip.open(f"{OUT}/ecom-eventi.tsv.gz", "wt") as out:
        for url in urls:
            r = get(url, timeout=600)
            c = Counter(r)
            raw = io.BufferedReader(c, 1 << 20)
            if url.endswith(".gz"):
                raw = gzip.GzipFile(fileobj=raw)
            if url.endswith(".parquet"):
                import pyarrow.parquet as pq
                data = r.read()
                c.n = len(data)
                tb = pq.read_table(io.BytesIO(data))
                rows = tb.to_pylist()
            else:
                rows = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"))
            n = 0
            for row in rows:
                n += 1
                if fmt == "rees46":
                    out.write(f"{row.get('event_time')}\t{row.get('user_id')}\t{row.get('event_type')}\t{row.get('product_id')}\t{row.get('user_session')}\n")
                else:  # retailrocket
                    out.write(f"{row.get('timestamp')}\t{row.get('visitorid')}\t{row.get('event')}\t{row.get('itemid')}\t{row.get('transactionid')}\n")
                if c.n > cap:
                    break
            tot += c.n
            print(url, n, c.n, flush=True)
            if tot > cap:
                break
    json.dump({"url": urls, "byte_scaricati": tot}, open(f"{OUT}/ecom-conti.json", "w"), indent=1)


import urllib.parse  # noqa: E402

if PARTE == "gh":
    do_gh()
elif PARTE == "ecom":
    do_ecom()
