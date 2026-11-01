#!/usr/bin/env python3
"""Reads results/*.jsonl (unified spec tests) and writes results/riepilogo.json and RISULTATI.md.

Every failure is put by hand-written rules (below, each with its reason) into one of:
  bug-dati       the engine writes or keeps the wrong data
  bug-risposta   the engine answers with wrong data (nothing wrong is stored)
  mancante       a MongoDB feature the engine does not have
  diverso        the feature exists, but an answer, an error or a check differs from MongoDB
A failure that no rule covers stays "da-classificare" (there should be none).
"""
import collections
import glob
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

RULES = [
    # (category, label, test(result) -> bool)
    ("bug-dati", "updateOne/replaceOne con sort (novità 8.0): il sort è ignorato e viene modificato un altro "
                 "documento",
     lambda r: r["file"] in ("updateOne-sort.json", "replaceOne-sort.json", "bulkWrite-updateOne-sort.json",
                             "bulkWrite-replaceOne-sort.json")),
    ("bug-dati", "transazione: un findAndModify che non trova niente cancella al commit le scritture precedenti "
                 "della stessa transazione (il commit risponde ok)",
     lambda r: r["area"] == "transazioni" and r["file"] == "findOneAndDelete.json"),
    ("bug-risposta", "sessioni snapshot: le letture vedono scritture fatte dopo l'inizio dello snapshot",
     lambda r: r["file"] == "snapshot-sessions.json"),
    ("mancante", "bulkWrite tra collezioni (comando bulkWrite di MongoDB 8.0): il server non ha il comando e si "
                 "dichiara 7.0, quindi i driver lo rifiutano prima di inviarlo",
     lambda r: "requires MongoDB server version 8.0" in r["reason"] or
     (r["file"].startswith("client-bulkWrite") and "expected writeErrors" in r["reason"])),
    ("mancante", "$out e $merge in un aggregate a livello di database (es. dopo $documents)",
     lambda r: "Unrecognized pipeline stage name: '$out'" in r["reason"] or
     "Unrecognized pipeline stage name: '$merge'" in r["reason"]),
    ("mancante", "creare collezioni e indici dentro una transazione (MongoDB lo permette dalla 4.4)",
     lambda r: "in a multi-document transaction" in r["reason"] and ("'create'" in r["reason"] or
                                                                       "'createIndexes'" in r["reason"])),
    ("mancante", "campo updateDescription.disambiguatedPaths degli eventi di change stream",
     lambda r: "disambiguatedPaths" in r["reason"]),
    ("diverso", "sessioni snapshot: le scritture e i comandi di elenco con readConcern snapshot sono accettati "
                "(MongoDB li rifiuta)",
     lambda r: r["file"] == "snapshot-sessions-unsupported-ops.json"),
    ("diverso", "let del comando aggregate: {$literal: '$bar'} è rifiutato come accesso a un campo",
     lambda r: r["file"] == "aggregate-let.json"),
    ("diverso", "$out con readConcern linearizable: accettato (MongoDB dà errore)",
     lambda r: r["file"] == "aggregate-out-readConcern.json"),
    ("diverso", "_id con chiave che inizia per $: accettato (MongoDB lo rifiuta)",
     lambda r: r["file"] == "insertOne-dots_and_dollars.json"),
    ("diverso", "collezione timeseries: _id duplicati rifiutati (in MongoDB le timeseries non hanno indice "
                "unico su _id)",
     lambda r: r["file"] == "timeseries-collection.json"),
    ("diverso", "change stream: stadio non valido nel pipeline accettato all'apertura",
     lambda r: "invalid aggrega" in r["test"]),
    ("diverso", "change stream: togliere _id con $project non dà l'errore 280 del server",
     lambda r: "expected error code 280" in r["reason"]),
    ("diverso", "change stream: array troncato riportato come updatedFields invece di truncatedArrays",
     lambda r: r["test"] == "Test array truncation"),
    ("diverso", "find con min/max e hint {_id: 1}: il server dice che manca l'hint",
     lambda r: "When using min()/max() a hint" in r["reason"]),
    ("diverso", "find con returnKey: documento vuoto invece della chiave dell'indice usato",
     lambda r: "showRecordId and returnKey" in r["test"]),
]

SKIP_LABELS = {
    "failpoint": "richiedono configureFailPoint, un aggancio di prova del server per simulare guasti (in mongod "
                 "va acceso con enableTestCommands, in Atlas non c'è): il motore non lo ha",
    "req:maxServerVersion": "valgono solo per versioni di MongoDB più vecchie della 8.0",
    "req:minServerVersion": "valgono solo per versioni più nuove della 8.0",
    "req:topology": "richiedono uno sharded cluster o un load balancer",
    "req:auth": "richiedono il server con autenticazione accesa (qui spenta)",
    "req:secondario": "richiedono un nodo secondario: il nostro replica set ha un solo membro",
    "req:serverParameter": "richiedono un parametro del server (per esempio acceptApiVersion2)",
    "req:atlas-search": "indici di ricerca di Atlas (Atlas Search): fuori dal perimetro",
    "runner": "operazioni che il nostro runner non implementa (limite della misura, non del motore)",
    "spec-skip": "segnati da saltare nel file stesso",
}


def family(reason):
    head = reason.split(" ")[0]
    if head.startswith("runner:"):
        return "runner"
    return head


def main():
    areas = {}
    fails = []
    for p in sorted(glob.glob(os.path.join(HERE, "results", "*.jsonl"))):
        name = os.path.basename(p)[:-6]
        if name in ("funzioni", "driver"):
            continue
        rs = [json.loads(l) for l in open(p)]
        c = collections.Counter(r["status"] for r in rs)
        skips = collections.Counter(family(r.get("reason", "")) for r in rs if r["status"] == "SKIP")
        runner_ops = collections.Counter(r["reason"] for r in rs if r["status"] == "SKIP" and
                                         r["reason"].startswith("runner:"))
        cats = collections.Counter()
        for r in rs:
            if r["status"] != "FAIL":
                continue
            for cat, label, f in RULES:
                if f(r):
                    r["categoria"], r["etichetta"] = cat, label
                    break
            else:
                r["categoria"], r["etichetta"] = "da-classificare", r["reason"][:200]
            cats[r["categoria"]] += 1
            fails.append(r)
        areas[name] = {"totale": len(rs), "passati": c["PASS"], "falliti": c["FAIL"], "saltati": c["SKIP"],
                       "percentuale_sui_eseguiti": round(100 * c["PASS"] / (c["PASS"] + c["FAIL"]), 1)
                       if c["PASS"] + c["FAIL"] else None,
                       "fallimenti_per_categoria": dict(cats), "saltati_per_motivo": dict(skips),
                       "runner_non_implementato": dict(runner_ops)}
    tot = collections.Counter()
    for a in areas.values():
        for k in ("totale", "passati", "falliti", "saltati"):
            tot[k] += a[k]
    by_label = collections.defaultdict(list)
    for r in fails:
        by_label[(r["categoria"], r["etichetta"])].append(f"{r['area']}/{r['file']}: {r['test']}")
    out = {"aree": areas, "totale": dict(tot),
           "percentuale_totale_sui_eseguiti": round(100 * tot["passati"] / (tot["passati"] + tot["falliti"]), 1),
           "fallimenti": [{"categoria": c, "etichetta": l, "test": t} for (c, l), t in sorted(by_label.items())]}
    json.dump(out, open(os.path.join(HERE, "results", "riepilogo.json"), "w"), indent=1, ensure_ascii=False)
    write_md(out)


ORDER = ["bug-dati", "bug-risposta", "mancante", "diverso", "da-classificare"]
CAT_IT = {"bug-dati": "Bug che toccano i dati", "bug-risposta": "Bug di risposta (dati giusti, risposta sbagliata)",
          "mancante": "Funzionalità mancanti", "diverso": "Comportamenti diversi",
          "da-classificare": "Da classificare"}


def write_md(o):
    L = ["# Risultati grezzi dei test ufficiali (formato unified)", "",
         "Generato da `classifica.py` a partire da `results/*.jsonl`. Numeri MISURATI su questa macchina.", "",
         f"In tutto: {o['totale']['totale']} test, {o['totale']['passati']} passati, {o['totale']['falliti']} "
         f"falliti, {o['totale']['saltati']} saltati. Sui test eseguiti passa il "
         f"{o['percentuale_totale_sui_eseguiti']}%.", "", "## Per area", ""]
    for name, a in o["aree"].items():
        pct = "nessun test eseguito" if a["percentuale_sui_eseguiti"] is None else \
            f"{a['percentuale_sui_eseguiti']}% dei test eseguiti"
        L.append(f"- **{name}**: {a['passati']} passati, {a['falliti']} falliti, {a['saltati']} saltati "
                 f"({pct}).")
        if a["fallimenti_per_categoria"]:
            L.append("  Falliti: " + ", ".join(f"{v} {k}" for k, v in a["fallimenti_per_categoria"].items()) + ".")
        if a["saltati_per_motivo"]:
            L.append("  Saltati: " + ", ".join(f"{v} {k}" for k, v in a["saltati_per_motivo"].items()) + ".")
    L += ["", "## Fallimenti, dal più grave", ""]
    for cat in ORDER:
        items = [f for f in o["fallimenti"] if f["categoria"] == cat]
        if not items:
            continue
        L += [f"### {CAT_IT[cat]}", ""]
        for f in items:
            L.append(f"- {f['etichetta']} ({len(f['test'])} test)")
            for t in f["test"]:
                L.append(f"  - {t}")
        L.append("")
    L += ["## Perché i test saltati sono saltati", ""]
    for k, v in SKIP_LABELS.items():
        L.append(f"- `{k}`: {v}.")
    open(os.path.join(HERE, "RISULTATI.md"), "w").write("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
