"""Misura M9: quante letture il maître potrebbe servire già pronte.

Legge un flusso di eventi e simula, in un solo passaggio, per ogni combinazione
(cache dell'app sì/no) x (finestra del maître W):

  - "collezione": la risposta tenuta pronta vale finché nessuna scrittura tocca
    la stessa collezione (invalidazione prudente per collezione);
  - "intervallo": vale finché nessuna scrittura tocca uno dei gettoni letti dalla
    query (intervallo di chiavi, stimato dalle chiavi toccate);
  - "tetto": qualunque ripetizione esatta entro W, anche con scritture in mezzo
    (è il massimo teorico se il maître sa correggere la risposta col quaderno).

Evento: (t, db, coll, tipo, qkey, gettoni)
  tipo in {"point","filter","count","agg"} per le letture, "write" per le scritture.
  qkey: identità esatta della query (forma + parametri), None per le scritture.
  gettoni: per le letture i gettoni letti, per le scritture i gettoni toccati.

La cache dell'app: TTL 60 s sull'identità esatta della query, senza invalidazione
sulle scritture (il caso che toglie più ripetizioni, quindi il più sfavorevole al
maître). Le letture assorbite dalla cache dell'app non arrivano al database.
"""
from collections import OrderedDict, defaultdict

KINDS = ("point", "filter", "count", "agg")
POLICIES = ("collezione", "intervallo", "tetto")


class Combo:
    def __init__(self, app_ttl, window):
        self.app_ttl = app_ttl          # 0 = nessuna cache dell'app
        self.window = window            # secondi
        self.app = {}                   # (db,qkey) -> scadenza
        self.ent = {p: OrderedDict() for p in POLICIES}  # (db,qkey) -> (t_uso, seq, ver_coll)
        self.reads = defaultdict(int)          # letture totali per tipo
        self.absorbed = defaultdict(int)       # assorbite dalla cache dell'app
        self.served = {p: defaultdict(int) for p in POLICIES}
        self.peak_live = 0
        self.per_app = defaultdict(lambda: [0, 0, 0, 0, 0])  # letture, al_db, coll, interv, tetto
        self.last_sweep = -1


class Analyzer:
    def __init__(self, combos):
        self.combos = [Combo(a, w) for a, w in combos]
        self.seq = 0
        self.tok_seq = {}                # gettone -> seq dell'ultima scrittura
        self.coll_ver = defaultdict(int)  # (db,coll) -> versione
        self.writes = 0
        self.t_last = 0

    def feed(self, ev):
        t, db, coll, kind, qkey, toks = ev
        self.t_last = t
        if kind == "write":
            self.seq += 1
            self.writes += 1
            s = self.seq
            for tk in toks:
                self.tok_seq[tk] = s
            self.coll_ver[(db, coll)] += 1
            return
        k = (db, qkey)
        cv = self.coll_ver[(db, coll)]
        tseq = self.tok_seq
        appname = db.split("/")[0]
        for c in self.combos:
            c.reads[kind] += 1
            pa = c.per_app[appname]
            pa[0] += 1
            if c.app_ttl:
                exp = c.app.get(k)
                if exp is not None and exp > t:
                    c.absorbed[kind] += 1
                    continue
                c.app[k] = t + c.app_ttl
            pa[1] += 1
            for p in POLICIES:
                od = c.ent[p]
                e = od.get(k)
                ok = False
                if e is not None and t - e[0] <= c.window:
                    if p == "tetto":
                        ok = True
                    elif p == "collezione":
                        ok = e[2] == cv
                    else:
                        es = e[1]
                        ok = True
                        for tk in toks:
                            if tseq.get(tk, 0) > es:
                                ok = False
                                break
                if ok:
                    c.served[p][kind] += 1
                    pa[2 + POLICIES.index(p)] += 1
                # dopo una lettura (servita o ricalcolata) la risposta è valida adesso
                od[k] = (t, self.seq, cv)
                od.move_to_end(k)
            if t - c.last_sweep >= 30:
                c.last_sweep = t
                for p in POLICIES:
                    od = c.ent[p]
                    while od:
                        kk, e = next(iter(od.items()))
                        if t - e[0] > c.window:
                            od.popitem(last=False)
                        else:
                            break
                live = len(c.ent["intervallo"])
                if live > c.peak_live:
                    c.peak_live = live
                if c.app_ttl and len(c.app) > 200000:
                    c.app = {kk: v for kk, v in c.app.items() if v > t}

    def result(self):
        out = []
        for c in self.combos:
            tot = sum(c.reads.values())
            absb = sum(c.absorbed.values())
            db_reads = tot - absb
            r = {
                "app_ttl": c.app_ttl, "window": c.window,
                "letture": tot, "assorbite_app": absb, "letture_al_db": db_reads,
                "scritture": self.writes, "picco_risposte_pronte": c.peak_live,
                "per_tipo": {},
            }
            for p in POLICIES:
                s = sum(c.served[p].values())
                r[p] = s / db_reads if db_reads else 0.0
            for kd in KINDS:
                n = c.reads[kd] - c.absorbed[kd]
                if c.reads[kd] == 0:
                    continue
                r["per_tipo"][kd] = {
                    "letture": c.reads[kd], "al_db": n,
                    **{p: (c.served[p][kd] / n if n else 0.0) for p in POLICIES},
                }
            r["per_app"] = {a: v for a, v in sorted(c.per_app.items())}
            out.append(r)
        return out


def fmt(results, title):
    lines = [f"== {title}"]
    for r in results:
        lines.append(
            f"cache_app={r['app_ttl']}s W={r['window']}s letture={r['letture']} "
            f"assorbite_app={r['assorbite_app']} al_db={r['letture_al_db']} scritture={r['scritture']} "
            f"| collezione={r['collezione']:.1%} intervallo={r['intervallo']:.1%} tetto={r['tetto']:.1%} "
            f"| picco_pronte={r['picco_risposte_pronte']}")
        for kd, v in r["per_tipo"].items():
            lines.append(
                f"    {kd:6s} letture={v['letture']} al_db={v['al_db']} "
                f"collezione={v['collezione']:.1%} intervallo={v['intervallo']:.1%} tetto={v['tetto']:.1%}")
    return "\n".join(lines)
