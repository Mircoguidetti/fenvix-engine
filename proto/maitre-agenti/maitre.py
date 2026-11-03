"""M18: il maître (cache dei risultati corretta dal quaderno) su tracce di agenti AI.

Stesso metodo di M9 (misura/maitre/analizza.py sul branch misura-maitre), allargato:
  - cache dell'app prima del maître: TTL 60 s sull'identità esatta, senza buttare la
    risposta quando arriva una scrittura (l'ipotesi M9, la più sfavorevole al maître);
  - finestra W: il maître tiene una risposta finché qualcuno la richiede almeno ogni W s;
  - tre regole di validità, come M9:
      "collezione": nessuna scrittura nella collezione dopo che la risposta è stata fatta;
      "intervallo": nessuna scrittura sui gettoni (chiavi o pezzi di indice) letti;
      "tetto": ogni ripetizione esatta entro W, anche con scritture in mezzo (massimo
               teorico se il maître corregge il piatto con le righe del quaderno);
  - in più, per le letture che arrivano al database e NON sono ripetizioni esatte:
      "parametri": la stessa forma (query con le costanti tolte) è già passata entro W.
        Il maître non può dare la risposta (i parametri sono diversi): è il tetto di una
        vista materializzata per forma (stile Noria) o di un piano già pronto.
  - "sottoquery": lettura non ripetuta esatta, ma con almeno un pezzo (sotto-SELECT, tabella con
    il suo filtro, prefisso di una pipeline MongoDB) già calcolato entro W. È il tetto di un maître
    che riusa i pezzi intermedi (Berkeley); non dice quanto lavoro si risparmia.
  - memoria: picco delle risposte tenute pronte (regola tetto, la più grande) e dei byte.

Evento: dict con
  t      secondi (float)
  db     ambito: chi condivide il maître e la cache dell'app (database, o sessione, ...)
  coll   collezione (per le letture la collezione principale)
  kind   "read" o "write"
  qkey   identità esatta della lettura (stringa)
  shape  forma della lettura senza costanti (stringa)
  toks   gettoni letti (lettura) o toccati (scrittura); "coll|campo|*" = tutti i valori
  colls  (scrittura) collezioni toccate; (lettura, opzionale) collezioni lette
  size   byte della risposta (lettura)
  grp    etichetta per i conti per gruppo
"""
from collections import OrderedDict, defaultdict

POLICIES = ("collezione", "intervallo", "tetto")


def fam(tok):
    i = tok.rfind("|")
    return tok[:i] if i > 0 else tok


class Combo:
    def __init__(self, app_ttl, window):
        self.app_ttl = app_ttl
        self.window = window
        self.app = {}
        self.ent = {p: OrderedDict() for p in POLICIES}  # key -> (t_uso, seq, ver_colls, size)
        self.shapes = {}                                  # (db,shape) -> t ultimo uso
        self.reads = 0
        self.absorbed = 0
        self.served = defaultdict(int)
        self.param = 0
        self.subq = 0
        self.subseen = {}                                 # (db,sotto-unità) -> t ultimo uso
        self.peak_live = 0
        self.peak_bytes = 0
        self.last_sweep = -1e18
        self.last_app_clean = -1e18
        self.grp = defaultdict(lambda: [0, 0, 0, 0, 0, 0])  # letture, al_db, coll, interv, tetto, param


class Analyzer:
    def __init__(self, combos):
        self.combos = [Combo(a, w) for a, w in combos]
        self.seq = 0
        self.tok_seq = {}
        self.fam_seq = {}
        self.coll_ver = defaultdict(int)
        self.writes = 0

    def _cv(self, db, colls):
        return tuple(self.coll_ver[(db, c)] for c in colls)

    def feed(self, ev):
        t, db = ev["t"], ev["db"]
        if ev["kind"] == "write":
            self.seq += 1
            self.writes += 1
            s = self.seq
            for tk in ev["toks"]:
                if tk.endswith("|*"):
                    self.fam_seq[(db, tk[:-2])] = s
                else:
                    self.tok_seq[(db, tk)] = s
            for c in ev.get("colls") or [ev["coll"]]:
                self.coll_ver[(db, c)] += 1
            return
        k = (db, ev["qkey"])
        colls = ev.get("colls") or [ev["coll"]]
        cv = self._cv(db, colls)
        toks = ev["toks"]
        size = ev.get("size", 0)
        g = ev.get("grp", "")
        for c in self.combos:
            c.reads += 1
            pg = c.grp[g]
            pg[0] += 1
            if c.app_ttl:
                exp = c.app.get(k)
                if exp is not None and exp > t:
                    c.absorbed += 1
                    continue
                c.app[k] = t + c.app_ttl
            pg[1] += 1
            hit_tetto = False
            for i, p in enumerate(POLICIES):
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
                            if self.tok_seq.get((db, tk), 0) > es or self.fam_seq.get((db, fam(tk)), 0) > es:
                                ok = False
                                break
                if ok:
                    c.served[p] += 1
                    pg[2 + i] += 1
                    if p == "tetto":
                        hit_tetto = True
                od[k] = (t, self.seq, cv, size)
                od.move_to_end(k)
            sk = (db, ev["shape"])
            ts = c.shapes.get(sk)
            if not hit_tetto and ts is not None and t - ts <= c.window:
                c.param += 1
                pg[5] += 1
            c.shapes[sk] = t
            subs = ev.get("subs")
            if subs:
                if not hit_tetto:
                    for su in subs:
                        tt = c.subseen.get((db, su))
                        if tt is not None and t - tt <= c.window:
                            c.subq += 1
                            break
                for su in subs:
                    c.subseen[(db, su)] = t
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
                od = c.ent["tetto"]
                live = len(od)
                if live > c.peak_live:
                    c.peak_live = live
                    c.peak_bytes = max(c.peak_bytes, sum(e[3] for e in od.values()))
                elif live > 0.8 * c.peak_live:
                    b = sum(e[3] for e in od.values())
                    if b > c.peak_bytes:
                        c.peak_bytes = b
                if c.app_ttl and len(c.app) > 200000 and t - c.last_app_clean >= c.app_ttl:
                    c.last_app_clean = t
                    c.app = {kk: v for kk, v in c.app.items() if v > t}

    def result(self):
        out = []
        for c in self.combos:
            dbr = c.reads - c.absorbed
            r = {
                "app_ttl": c.app_ttl, "window": c.window,
                "letture": c.reads, "assorbite_app": c.absorbed, "letture_al_db": dbr,
                "scritture": self.writes,
                "picco_risposte_pronte": c.peak_live, "picco_byte_pronti": c.peak_bytes,
            }
            for p in POLICIES:
                r[p] = c.served[p] / dbr if dbr else 0.0
            r["parametri_in_piu"] = c.param / dbr if dbr else 0.0
            r["sottoquery_in_piu"] = c.subq / dbr if dbr else 0.0
            r["per_gruppo"] = {g: {"letture": v[0], "al_db": v[1],
                                   "collezione": v[2] / v[1] if v[1] else 0,
                                   "intervallo": v[3] / v[1] if v[1] else 0,
                                   "tetto": v[4] / v[1] if v[1] else 0,
                                   "parametri_in_piu": v[5] / v[1] if v[1] else 0}
                               for g, v in sorted(c.grp.items())}
            out.append(r)
        return out


def run(events, combos):
    a = Analyzer(combos)
    for ev in sorted(events, key=lambda e: e["t"]):
        a.feed(ev)
    return a.result()


def fmt(results, title):
    lines = [f"== {title}"]
    for r in results:
        lines.append(
            f"cache_app={r['app_ttl']}s W={r['window']}s letture={r['letture']} "
            f"assorbite_app={r['assorbite_app']} al_db={r['letture_al_db']} scritture={r['scritture']} "
            f"| collezione={r['collezione']:.1%} intervallo={r['intervallo']:.1%} tetto={r['tetto']:.1%} "
            f"| +parametri={r['parametri_in_piu']:.1%} +sottoquery={r['sottoquery_in_piu']:.1%} | picco_pronte={r['picco_risposte_pronte']} "
            f"picco_byte={r['picco_byte_pronti']}")
    return "\n".join(lines)
