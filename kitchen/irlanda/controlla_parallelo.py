"""P1 Irlanda v2 · dopo il giro (0 $, legge solo i file): nel parallelo i clienti partiti "insieme" avevano davvero
una connessione pronta ciascuno? Dal file della fase parallelo dello script (eventi del pool del driver, salvati per
livello) conta, per ogni livello:
  - "connessioni pronte prima": quelle aperte e scaldate prima del primo lotto (la riga dello script);
  - per ogni lotto freddo: quante connessioni NUOVE si sono aperte mentre il lotto girava (giusto: 0, cioe' nessuna
    lettura ha dovuto aspettare l'apertura di una connessione); per informazione anche quante letture erano "in volo"
    insieme al massimo e quante connessioni diverse hanno usato. Se una lettura finisce prima che un altro filo
    parta (Python fa partire i fili uno alla volta), quel filo riusa la connessione appena liberata: non e' una
    connessione mancante, e' il limite di un processo Python nel far partire N fili nello stesso istante.
Uso: controlla_parallelo.py CARTELLA_DEL_GIRO   (stampa; codice 0 = tutto giusto, 1 = qualcosa no, 2 = file mancante)"""
import glob
import json
import os
import sys


def main():
    cart = sys.argv[1]
    fs = sorted(glob.glob(os.path.join(cart, "fase_*parallelo*.json")))
    if not fs:
        print(f"PARALLELO: nessun file della fase parallelo in {cart}")
        sys.exit(2)
    rec = json.load(open(fs[-1]))
    tutto_ok = True
    for lv in rec.get("livelli", []):
        liv = lv.get("livello")
        if "errore" in lv:
            print(f"L{liv}: ERRORE nel livello: {lv['errore']}")
            tutto_ok = False
            continue
        pool = (lv.get("eventi_driver") or {}).get("pool", [])
        pronte = lv.get("connessioni_pronte")
        righe, ok_liv = [], pronte is not None and pronte >= liv
        for b in range(lv.get("lotti_fatti", 0)):
            ms = [m for m in lv.get("misure", []) if m.get("lotto") == b and m.get("tipo") == "freddo"]
            if not ms:
                continue
            t0, t1 = min(m["inizio"] for m in ms), max(m["fine"] for m in ms)
            # gli eventi hanno l'ora in ISO con i microsecondi: confronto di stringhe dello stesso formato
            dentro = [e for e in pool if t0[:26] <= e["t"][:26] <= t1[:26]]
            prese = {e.get("connessione") for e in dentro if e["evento"] == "connessione_presa"}
            aperte = sum(1 for e in dentro if e["evento"] == "connessione_aperta")
            q = ms[0].get("query")
            ev = sorted([(m["inizio"][:26], 1) for m in ms] + [(m["fine"][:26], -1) for m in ms], key=lambda x: (x[0], x[1]))
            volo = picco = 0
            for _, d in ev:
                volo += d
                picco = max(picco, volo)
            ok = aperte == 0
            ok_liv = ok_liv and ok
            righe.append(f"    lotto {b} {q}: {len(ms)} letture fredde, connessioni diverse usate {len(prese)}, "
                         f"in volo insieme al massimo {picco}, aperte durante il lotto {aperte} -> {'OK' if ok else 'NO'}")
        print(f"L{liv}: connessioni pronte prima {pronte} (servono {liv}) -> {'OK' if ok_liv else 'NO'}")
        print("\n".join(righe))
        tutto_ok = tutto_ok and ok_liv
    print("PARALLELO: " + ("connessioni tutte pronte prima di ogni livello, nessuna aperta durante i lotti" if tutto_ok
                           else "NON tutte le letture avevano una connessione pronta: vedi le righe NO"))
    sys.exit(0 if tutto_ok else 1)


if __name__ == "__main__":
    main()
