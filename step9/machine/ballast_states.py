"""Step 9 · a state file in the founder's script's format for each generated ballast database, so the script's read
rounds (`giro ... --solo-fasi inizio,Q2,Q3,Q1,Q1d,Q4`) run on the ballast unchanged: the fields `carica` writes
(versione_script, db, server, config, caricamento with finito, clienti with nomi_da, n, riservati, byte, q2_id,
fine_attivita), filled from the plan and the generator's report. Two extra keys say where the data came from
("generato" and "generatore"); the script ignores keys it does not know.
Usage: ballast_states.py PLAN_JSON GEN_DIR STATES_DIR SERVER_VERSION
  writes STATES_DIR/<db>/risultati_p1.json for every database whose <db>.gen.json exists; exit 1 if one is missing."""
import datetime as dt
import json
import os
import sys

plan_f, gen_dir, states, server_version = sys.argv[1:5]
plan = json.load(open(plan_f))
missing = []
for d in plan["databases"]:
    g_f = os.path.join(gen_dir, d["db"] + ".gen.json")
    if not os.path.exists(g_f):
        missing.append(d["db"])
        continue
    g = json.load(open(g_f))
    assert g["customers"] == len(d["taglie"]), (d["db"], g["customers"], len(d["taglie"]))
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    line = d["line"]
    st = {
        "versione_script": plan["script"]["versione"], "db": d["db"],
        "server": {"versione": server_version, "modules": []},
        "config": {"gb": d["gb"], "mb_cliente": d["mb_cliente"], "clienti": len(d["taglie"]), "riservati": len(d["riservati"]),
                   "thread": int(line[line.index("--thread") + 1]) if "--thread" in line else 4, "lotto": 1000,
                   "seme": d["seed"], "tetto_logico_gb": float(line[line.index("--tetto-logico-gb") + 1]) if "--tetto-logico-gb" in line else 50.0},
        "caricamento": {"inizio": now, "finito": True, "fine": now, "motivo_fine": "obiettivo raggiunto",
                        "byte_bson": g["byte_bson"], "documenti": g["documenti"], "per_collezione": g["per_collezione"],
                        "errori": 0, "secondi": round(g["secondi"], 1), "mb_s": round(g["byte_bson"] / (1 << 20) / max(1e-9, g["secondi"]), 3)},
        "clienti": {"nomi_da": "cli-%06d", "n": len(d["taglie"]), "riservati": d["riservati"], "byte": g["byte"],
                    "q2_id": g["q2_id"], "fine_attivita": {"riservati": plan["fine_dormienti"], "altri": 1.0}},
        "generato": True,
        "generatore": {"how": "mongo-server --ballast (sealed runs, our manifest)", "x0": g["x0"], "build": g.get("build")},
    }
    os.makedirs(os.path.join(states, d["db"]), exist_ok=True)
    json.dump(st, open(os.path.join(states, d["db"], "risultati_p1.json"), "w"), indent=1)
print(f"ballast states: {len(plan['databases']) - len(missing)} written" + (f"; MISSING {missing}" if missing else ""))
sys.exit(1 if missing else 0)
