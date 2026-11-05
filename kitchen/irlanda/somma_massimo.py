"""P1 Irlanda v2 · somma della fase carico_massimo fatta da PIU' copie dello script insieme (0 $, legge solo i file).
Per ogni parte (letture, lotti_w1, lotti_majority):
  - somma semplice delle operazioni al secondo di ogni copia;
  - somma nella finestra COMUNE (dal piu' tardo inizio al piu' presto fine delle copie), dai contatori ogni 30 s di
    ogni copia: e' il numero da usare, perche' le copie partono a pochi secondi l'una dall'altra;
  - CPU della macchina nella finestra comune (cpu-macchina.log, da /proc/stat), CPU di ogni copia (% di un core),
    CPU di Atlas (serverStatus del primario, dalla copia: % di un core e core del nodo da hostInfo), tempi visti dalla
    macchina (p50 e p99 per operazione);
  - il verdetto: chi e' stato il limite (MACCHINA, COPIE, ATLAS o NON SO), con la regola scritta accanto.
Uso: somma_massimo.py CARTELLA_DELLE_COPIE FILE_CPU_MACCHINA   (CARTELLA/NN/dati/giro_finale_*/fase_*carico_massimo*.json)"""
import datetime as dt
import glob
import json
import os
import statistics
import sys

SOGLIA_MACCHINA = 80.0   # % di tutte le vCPU (prompt del coordinatore: sotto l'80%)
SOGLIA_COPIA = 85.0      # % di un core: un processo Python con il GIL non va oltre circa 100%
SOGLIA_ATLAS = 85.0      # % dei core del nodo primario di Atlas


def t(s):
    return dt.datetime.strptime(s[:26], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=dt.timezone.utc).timestamp()


def cerca(x, chiave):
    if isinstance(x, dict):
        if chiave in x and isinstance(x[chiave], (int, float)):
            return x[chiave]
        for v in x.values():
            r = cerca(v, chiave)
            if r is not None:
                return r
    elif isinstance(x, list):
        for v in x:
            r = cerca(v, chiave)
            if r is not None:
                return r
    return None


def ops_al_tempo(pt, ts):
    """operazioni fatte dalla copia fino al tempo ts (interpolazione lineare tra i contatori ogni 30 s)."""
    pts = [(t(pt["inizio"]), 0)] + [(t(c["t"]), c["operazioni"]) for c in pt.get("campioni_30s", [])] + \
          [(t(pt["fine"]), pt["operazioni"])]
    pts.sort()
    if ts <= pts[0][0]:
        return 0.0
    for (a, na), (b, nb) in zip(pts, pts[1:]):
        if a <= ts <= b:
            return na + (nb - na) * ((ts - a) / (b - a) if b > a else 1.0)
    return float(pts[-1][1])


def v(x, d=1):
    return "-" if x is None else f"{x:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def main():
    cart, fcpu = sys.argv[1], sys.argv[2]
    copie = []
    for f in sorted(glob.glob(os.path.join(cart, "*", "dati", "giro_finale_*", "fase_*carico_massimo*.json"))):
        copie.append((f.split(os.sep)[-4], json.load(open(f))))
    if not copie:
        print(f"CARICO MASSIMO: nessun file della fase in {cart}")
        sys.exit(2)
    cpu = []
    if os.path.exists(fcpu):
        cpu = [json.loads(x) for x in open(fcpu) if x.strip()]
    core_atlas = None
    for _, r in copie:
        core_atlas = core_atlas or cerca(r.get("prima"), "numCores")
    print(f"CARICO MASSIMO a piu' copie: {len(copie)} copie dello script, ognuna con {copie[0][1].get('thread')} fili; "
          f"vCPU della macchina {cpu[0]['vcpu'] if cpu else 'NON SO'}; core del primario di Atlas "
          f"{core_atlas if core_atlas else 'NON SO'} (hostInfo)")
    esito = {"copie": len(copie), "core_atlas": core_atlas, "parti": {}}
    for parte in ("letture", "lotti_w1", "lotti_majority"):
        pts = [(n, next((p for p in r.get("parti", []) if p.get("parte") == parte), None)) for n, r in copie]
        pts = [(n, p) for n, p in pts if p and p.get("inizio") and p.get("fine")]
        if not pts:
            print(f"\n{parte}: nessuna copia l'ha fatta")
            continue
        somma = sum(p["operazioni_al_secondo"] for _, p in pts)
        a, b = max(t(p["inizio"]) for _, p in pts), min(t(p["fine"]) for _, p in pts)
        comune = (sum(ops_al_tempo(p, b) - ops_al_tempo(p, a) for _, p in pts) / (b - a)) if b - a > 10 else None
        doc_per_op = pts[0][1].get("documenti_per_operazione") or 1
        cc = [x["cpu"] for x in cpu if a <= t(x["t"]) <= b]
        cpu_copie = [p.get("mac_cpu_percento_di_un_core") for _, p in pts if p.get("mac_cpu_percento_di_un_core") is not None]
        atl = [(p.get("cpu_atlas") or {}).get("cpu_percento_di_un_core") for _, p in pts]
        atl = [x for x in atl if isinstance(x, (int, float))]
        atl_med = statistics.median(atl) if atl else None
        p50 = [((p.get("ms_dal_mac") or {}).get("p50")) for _, p in pts]
        p99 = [((p.get("ms_dal_mac") or {}).get("p99")) for _, p in pts]
        p50 = statistics.median([x for x in p50 if x is not None]) if any(x is not None for x in p50) else None
        p99 = statistics.median([x for x in p99 if x is not None]) if any(x is not None for x in p99) else None
        err = sum(p.get("errori", 0) for _, p in pts)
        cpu_max = max(cc) if cc else None
        cpu_media = statistics.mean(cc) if cc else None
        copia_med = statistics.median(cpu_copie) if cpu_copie else None
        atlas_pieno = core_atlas and atl_med is not None and atl_med >= SOGLIA_ATLAS * core_atlas
        if cpu_max is None:
            verdetto = "NON SO: manca la CPU della macchina"
        elif cpu_max >= SOGLIA_MACCHINA:
            verdetto = (f"MACCHINA: la CPU della macchina e' arrivata a {v(cpu_max)}% (soglia {v(SOGLIA_MACCHINA, 0)}%): "
                        f"il limite puo' essere la macchina, il numero NON e' il limite di Atlas")
        elif copia_med is not None and copia_med >= SOGLIA_COPIA:
            verdetto = (f"COPIE: ogni copia usa {v(copia_med)}% di un core (soglia {v(SOGLIA_COPIA, 0)}%): il limite e' "
                        f"Python in ogni copia; servivano piu' copie. Il numero e' un minimo per Atlas, non il suo limite")
        elif atlas_pieno:
            verdetto = (f"ATLAS: macchina sotto l'{v(SOGLIA_MACCHINA, 0)}% (al massimo {v(cpu_max)}%), copie sotto il "
                        f"{v(SOGLIA_COPIA, 0)}% di un core ({v(copia_med)}%), CPU del primario di Atlas {v(atl_med)}% di un "
                        f"core su {core_atlas} core: il limite e' la CPU di Atlas")
        else:
            verdetto = (f"ATLAS (non la CPU): macchina e copie con margine (al massimo {v(cpu_max)}%, copie {v(copia_med)}% "
                        f"di un core) e CPU di Atlas non piena ({v(atl_med)}% di un core su {core_atlas or 'NON SO'} core): "
                        f"le copie aspettano le risposte (p50 {v(p50, 2)} ms, p99 {v(p99, 2)} ms): il limite e' dentro Atlas "
                        f"ma non e' la CPU (code, ticket, disco o rete del cluster). NON SO quale")
        print(f"\n{parte}: {len(pts)} copie; somma semplice {v(somma)} operazioni/s; nella finestra comune di "
              f"{v(b - a, 0)} s {v(comune)} operazioni/s ({v(comune * doc_per_op if comune else None, 0)} documenti/s); "
              f"errori {err}")
        print(f"  macchina: CPU media {v(cpu_media)}%, massima {v(cpu_max)}% (campioni {len(cc)}); copie: CPU per copia "
              f"mediana {v(copia_med)}% di un core (min {v(min(cpu_copie) if cpu_copie else None)}, max "
              f"{v(max(cpu_copie) if cpu_copie else None)}); tempi visti dalla macchina p50 {v(p50, 2)} ms, p99 {v(p99, 2)} ms")
        print(f"  Atlas: CPU del primario {v(atl_med)}% di un core (mediana delle copie), core {core_atlas or 'NON SO'}")
        print(f"  VERDETTO: {verdetto}")
        esito["parti"][parte] = {"copie": len(pts), "somma_semplice": somma, "finestra_comune_s": b - a,
                                 "operazioni_al_secondo_comune": comune, "documenti_al_secondo_comune":
                                 comune * doc_per_op if comune else None, "errori": err, "cpu_macchina_media": cpu_media,
                                 "cpu_macchina_max": cpu_max, "cpu_copia_mediana": copia_med, "cpu_atlas_mediana": atl_med,
                                 "p50_ms": p50, "p99_ms": p99, "verdetto": verdetto,
                                 "per_copia": {n: {"operazioni_al_secondo": p["operazioni_al_secondo"],
                                                   "cpu_copia": p.get("mac_cpu_percento_di_un_core"),
                                                   "inizio": p["inizio"], "fine": p["fine"]} for n, p in pts}}
    with open(os.path.join(cart, "somma.json"), "w") as f:
        json.dump(esito, f, indent=1)


if __name__ == "__main__":
    main()
