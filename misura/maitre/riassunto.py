"""Riassunto dei risultati: catalogo (totale, per tipo, per app) e Twitter (totale pesato e mediana)."""
import json, statistics, sys, os

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def pct(x):
    return f"{100*x:.1f}%"


def catalogo(nome):
    d = json.load(open(f"risultati/catalogo-{nome}.json"))
    print(f"\n### catalogo {nome}  eventi={d['meta']['eventi']}")
    for r in d["risultati"]:
        print(f"cache_app={r['app_ttl']} W={r['window']} letture={r['letture']} al_db={r['letture_al_db']} "
              f"coll={pct(r['collezione'])} int={pct(r['intervallo'])} tetto={pct(r['tetto'])} pronte={r['picco_risposte_pronte']}")
        for k, v in r["per_tipo"].items():
            print(f"   {k:6s} al_db={v['al_db']} coll={pct(v['collezione'])} int={pct(v['intervallo'])} tetto={pct(v['tetto'])}")
        apps = r["per_app"]
        quote = []
        for a, (n, ndb, c, i, t) in apps.items():
            if ndb >= 50:
                quote.append((c / ndb, i / ndb, t / ndb, a, ndb))
        if r["app_ttl"] == 60 and r["window"] == 600:
            quote.sort(reverse=True)
            cs = [q[0] for q in quote]
            print(f"   app con almeno 50 letture al db: {len(quote)}; mediana coll={pct(statistics.median(cs))} "
                  f"int={pct(statistics.median([q[1] for q in quote]))}; "
                  f"sopra 50% coll: {sum(1 for x in cs if x > .5)}; 25-50%: {sum(1 for x in cs if .25 <= x <= .5)}; "
                  f"sotto 25%: {sum(1 for x in cs if x < .25)}")
            for q in quote[:15]:
                print(f"      {q[3]:28s} al_db={q[4]:8d} coll={pct(q[0])} int={pct(q[1])} tetto={pct(q[2])}")
            # le prime 5 app per letture al db
            big = sorted(quote, key=lambda q: -q[4])[:8]
            print("   app con più letture al db:")
            for q in big:
                print(f"      {q[3]:28s} al_db={q[4]:8d} coll={pct(q[0])} int={pct(q[1])} tetto={pct(q[2])}")


def twitter(nome):
    d = json.load(open(f"risultati/{nome}.json"))
    print(f"\n### {nome}: {len(d)} cluster")
    for idx, lab in enumerate(["senza cache app", "cache app 10 s", "cache app 60 s"]):
        rows = [v[idx] for v in d.values()]
        tot = sum(r["letture_al_db"] for r in rows)
        agg = {p: sum(r[p] * r["letture_al_db"] for r in rows) / tot for p in ("collezione", "intervallo", "tetto")}
        med = {p: statistics.median(r[p] for r in rows) for p in ("collezione", "intervallo", "tetto")}
        lunghi = [r for r in rows if r["durata_s"] >= 300]
        print(f"{lab}: letture al db={tot} pesato coll={pct(agg['collezione'])} int={pct(agg['intervallo'])} "
              f"tetto={pct(agg['tetto'])} | mediana coll={pct(med['collezione'])} int={pct(med['intervallo'])} "
              f"| cluster sopra 50% coll={sum(1 for r in rows if r['collezione'] > .5)} int={sum(1 for r in rows if r['intervallo'] > .5)}"
              f" | cluster con almeno 300 s: {len(lunghi)}"
              + (f", mediana int={pct(statistics.median(r['intervallo'] for r in lunghi))}" if lunghi else ""))
    dur = [v[0]["durata_s"] for v in d.values()]
    print(f"durata dei campioni: min {min(dur)} s, mediana {statistics.median(dur)} s, max {max(dur)} s")


if __name__ == "__main__":
    for x in sys.argv[1:]:
        if x.startswith("twitter"):
            twitter(x)
        else:
            catalogo(x)
