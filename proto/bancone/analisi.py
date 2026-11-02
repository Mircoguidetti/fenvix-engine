"""M15: legge i risultati grezzi (risultati/**/risultati.jsonl) e scrive il riassunto (risultati/riassunto.txt).
Conti STIMATI per un database da 1 TB a 20.000 letture al secondo, prezzi DICHIARATI in SOGLIE.md."""
import glob, json, sys, collections, math, os

BASE = os.path.dirname(os.path.abspath(__file__))
R = 20_000 * 86400 * 30            # letture al mese
TB_GB = 1024
P_GET_S3 = 0.0004 / 1000
P_GET_EX = 0.00003 / 1000
P_PUT_EX = 0.00113 / 1000
P_EX_GB = 0.11
P_EX_UP = 0.0032
P_EX_DOWN = 0.0006
BUCKETS = 2                        # Express in eu-west-1 solo in 2 zone (euw1-az1, euw1-az3)
SOGLIA_COSTO = 0.10 * 2086         # 10% della bolletta B1 (a)
GP3 = 0.088

def carica(globs):
    rows, info, warm, ver, lcs = {}, {}, collections.defaultdict(list), [], []
    for g in globs:
        for f in glob.glob(os.path.join(BASE, g), recursive=True):
            fam = f.split("risultati/")[1].split("/")[0] + "/" + f.split("/")[-2]
            for l in open(f):
                try:
                    d = json.loads(l)
                except Exception:
                    continue
                k = d.get("kind")
                if k == "info":
                    info[d["trace"]] = d
                elif k == "warm":
                    warm[d["trace"]].append(d)
                elif k == "verifica":
                    ver.append(d)
                elif k == "libcachesim":
                    lcs.append(d)
                elif k is None and "combo" in d:
                    rows[(d["trace"], d["combo"], d["b"])] = d   # l'ultima vince (la corsa piena dopo la verifica)
    return rows, info, warm, ver, lcs

def costo(d):
    """Costo al mese in più (STIMATO) degli attrezzi, e il costo base delle GET su S3 Standard."""
    md = d.get("medio_doppia", {})
    base_get = d["s3_gets_per_read"] * R * P_GET_S3
    doppie = md.get("extra_gets_per_read", 0) * R * P_GET_S3     # tetto: tutte al prezzo Standard
    pf = d["pf_gets_s3_per_read"] * R * P_GET_S3 + d["pf_gets_ex_per_read"] * R * P_GET_EX
    e = 1.0 if d.get("full") else d["e"]
    ex_sto = e * TB_GB * P_EX_GB * BUCKETS if (e > 0) else 0
    ex_get = (d["ex_gets_per_read"]) * R * P_GET_EX + d["ex_bytes_per_read"] * R / 1e9 * P_EX_DOWN
    if d.get("full"):
        # copia piena: le scritture arrivano a lotti da 4 MB (come le scatole), una PUT per lotto per bucket
        ex_put = d["ex_put_bytes_per_read"] * R / 1e9 * (P_EX_UP + P_PUT_EX * 1e9 / 4e6) * BUCKETS
    else:
        ex_put = (d["ex_puts_per_read"] * R * P_PUT_EX + d["ex_put_bytes_per_read"] * R / 1e9 * P_EX_UP) * BUCKETS
    extra = doppie + pf + ex_sto + ex_get + ex_put
    return dict(base_get=base_get, doppie=doppie, pf=pf, ex_sto=ex_sto, ex_get=ex_get, ex_put=ex_put, extra=extra)

def famiglia(trace):
    for p in ("alibaba", "tencent", "twitter", "meta"):
        if p in trace:
            return p
    return trace

def riassunto(rows, info, warm, ver, lcs, out):
    w = out.write
    traces = sorted({k[0] for k in rows})
    w("# Riassunto M15 (generato da analisi.py)\n\n")
    for t in traces:
        i = info.get(t, {})
        w(f"## {t}\n")
        if i:
            dur_h = (i["t_last"] - i["t_first"]) / 3600
            w(f"richieste {i['records']:,}, letture {i['read_records']:,}, durata {dur_h:.1f} h; al database: letture {i['db_reads']:,}, scritture {i['db_writes']:,}"
              f"{', assorbite dalla cache app ' + format(i['app_absorbed'], ',') + ' su ' + format(i['app_reads_in'], ',') if i.get('app_reads_in') else ''}; "
              f"dati toccati {i['foot_bytes']/1e9:.2f} GB (letti {i['read_foot_bytes']/1e9:.2f} GB)\n")
        combos = sorted({k[1] for k in rows if k[0] == t})
        for c in combos:
            w(f"  [{c}]\n")
            for b in (0.02, 0.05, 0.1, 0.2, 0.5):
                d = rows.get((t, c, b))
                if not d:
                    continue
                m, md, o, p = d["medio"], d["medio_doppia"], d["ottimista_doppia"], d["pessimista_doppia"]
                cs = costo(d)
                pf = ""
                if d["pf_units"]:
                    prec = d["pf_used"] / d["pf_units"]
                    pf = f" anticipo: precisione {prec:.2f}, sprecati {d['pf_wasted']/d['pf_units']:.2f}, in ritardo {d['pf_late']}, byte anticipati per lettura {d['pf_bytes_per_read']:.0f}"
                w(f"    b={b:<4} mancate {d['read_miss']:.4f} S3 {d['to_s3']:.4f} Express {d['to_ex']:.4f} | medio p50 {m['p50']:.2f} p99 {m['p99']:.2f} p999 {m['p999']:.1f}"
                  f" | doppia: ott p99 {o['p99']:.2f}, medio p99 {md['p99']:.2f} p999 {md['p999']:.1f}, pess p99 {p['p99']:.2f}; GET in più {md['extra_gets_per_read']:.4f}/lettura"
                  f" | gp3 p99 {d['medio_doppia_gp3']['p99']:.2f} | costo in più ${cs['extra']:.0f}/mese (GET base S3 ${cs['base_get']:.0f}){pf}\n")
        w("\n")
    # la taglia minima per p99 <= 10 ms, per combinazione e scenario
    w("# Taglia minima del bancone per p99 <= 10 ms (con richiesta doppia)\n")
    for t in traces:
        for c in sorted({k[1] for k in rows if k[0] == t}):
            res = []
            for sc in ("ottimista_doppia", "medio_doppia", "pessimista_doppia", "medio"):
                ok = [b for b in (0.02, 0.05, 0.1, 0.2, 0.5) if (t, c, b) in rows and rows[(t, c, b)][sc]["p99"] <= 10.0]
                res.append(f"{sc} {'≤' + str(min(ok)) if ok else '>0.5'}")
            w(f"  {t} [{c}]: " + ", ".join(res) + "\n")
    w("\n# Riscaldamento (bancone vuoto): tempo perché le mancate per finestra scendano entro 1,1 volte il valore stabile\n")
    for t, lst in warm.items():
        t0 = info.get(t, {}).get("t_first", 0)
        for d in lst:
            win = d["win"]
            if not win:
                continue
            mr = [m / r if r else 0 for _, r, m in win]
            half = mr[len(mr) // 2:]
            steady = sum(half) / len(half)
            tw = next((tt for (tt, r, m), x in zip(win, mr) if x <= steady * 1.1 + 0.002), None)
            w(f"  {t} [{d['combo']}] b={d['b']}: stabile {steady:.4f}, prima finestra {mr[0]:.4f}, raggiunto dopo {((tw or 0) - t0)/3600:.2f} h\n")
    w("\n# Verifica contro libCacheSim (stesso flusso, tutte le operazioni, pagina per pagina)\n")
    for v in ver:
        same = [x for x in lcs if x.get("trace") == v["trace"] and x["cap"] == v["cap"] and x["policy"].lower() == v["policy"]]
        lib = same[0]["miss_ratio"] if same else float("nan")
        w(f"  {v['trace']} {v['policy']} b={v['b']}: nostro {v['miss_ratio']:.4f}, libCacheSim {lib:.4f}\n")
    pol = collections.defaultdict(dict)
    for x in lcs:
        pol[(x.get("trace"), x["cap"])][x["policy"]] = x["miss_ratio"]
    w("\n# Politiche in libCacheSim (tasso di mancata, tutte le operazioni)\n")
    for (t, cap), d in sorted(pol.items(), key=lambda z: (str(z[0][0]), z[0][1])):
        w(f"  {t} cap {cap/1e9:.3f} GB: " + ", ".join(f"{k} {v:.4f}" for k, v in sorted(d.items(), key=lambda z: z[1])) + "\n")

if __name__ == "__main__":
    g = sys.argv[1:] or ["risultati/**/risultati.jsonl", "risultati/**/libcachesim.jsonl"]
    rows, info, warm, ver, lcs = carica(g)
    with open(os.path.join(BASE, "risultati", "riassunto.txt"), "w") as f:
        riassunto(rows, info, warm, ver, lcs, f)
    print(open(os.path.join(BASE, "risultati", "riassunto.txt")).read()[:3000])
