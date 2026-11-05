"""K2b · on the client machine, after our load and before any measurement: build the state files our rounds start from,
so that our kitchen reads EXACTLY the clients and the windows Atlas read in the same round.

The script chooses the clients of a round from the state file only: number of clients, dormant clients, which ones
have an order (q2_id not empty), which were already read ('usati', the .usati.log register, the old hot rounds in
'giri'), and the name of the round; the Q1d dates also use the bytes each client wrote ('byte'). The windows (Q3, Q4,
Q1d dates) come from a seed made of the script version, round name, query and client. So:
  - everything that decides the choice comes from the ATLAS state file (the one the Atlas round started from);
  - from OUR load come only the things that point into our data: the order _id of each client for Q2 (q2_id) and the
    _id of the 2 KB documents ('grandi'), plus our load record (kept next to the Atlas one).
Checks (FERMO = stop, nothing is measured): same database, same number of clients, same dormant clients, the same
clients with and without an order, the 2 KB collection with the same number of clients when Atlas has one.

Usage: merge_states.py ATLAS_STATES OURS OUT STATE [STATE ...]
  ATLAS_STATES/<s>/risultati_p1.json(+.usati.log), OURS/<s>/risultati_p1.json -> OUT/<s>/risultati_p1.json(+.usati.log)
Exit code 0 = all states merged; 1 = FERMO (the reason is printed)."""
import copy
import json
import os
import shutil
import sys

GB = 1 << 30


def main():
    atlas_dir, ours_dir, out_dir = sys.argv[1:4]
    states = sys.argv[4:]
    stops = []
    report = {}
    for s in states:
        a = json.load(open(os.path.join(atlas_dir, s, "risultati_p1.json")))
        o = json.load(open(os.path.join(ours_dir, s, "risultati_p1.json")))
        ca, co = a["clienti"], o["clienti"]
        why = []
        if a.get("db") != o.get("db"):
            why.append(f"database {o.get('db')} instead of {a.get('db')}")
        if ca["n"] != co["n"]:
            why.append(f"{co['n']} clients instead of {ca['n']}")
        if sorted(ca["riservati"]) != sorted(co["riservati"]):
            why.append("different dormant clients")
        none_a = {i for i, x in enumerate(ca["q2_id"]) if not x}
        none_o = {i for i, x in enumerate(co["q2_id"]) if not x}
        if none_a != none_o:
            why.append(f"clients without an order differ (Atlas {len(none_a)}, ours {len(none_o)})")
        ga, go = a.get("grandi") or {}, o.get("grandi") or {}
        if ga.get("finito") and not (go.get("finito") and go.get("clienti") == ga.get("clienti")):
            why.append(f"2 KB collection: Atlas {ga.get('clienti')} clients, ours {go.get('clienti')} "
                       f"(finished: {go.get('finito')})")
        if why:
            stops.append(f"{s}: " + "; ".join(why))
            continue
        m = copy.deepcopy(a)
        m["clienti"]["q2_id"] = list(co["q2_id"])
        m["clienti"]["byte_kitchen"] = co.get("byte")
        if go:
            m["grandi_atlas"] = ga or None
            m["grandi"] = go
        m["caricamento_atlas"] = a.get("caricamento")
        m["caricamento"] = o.get("caricamento")
        m["server_atlas"] = a.get("server")
        m["server"] = o.get("server")
        fa = (a.get("caricamento") or {}).get("byte_bson", 0) / (a["config"]["gb"] * GB)
        fo = (o.get("caricamento") or {}).get("byte_bson", 0) / (o["config"]["gb"] * GB)
        m["kitchen_merge"] = {"atlas_fraction": round(fa, 6), "kitchen_fraction": round(fo, 6),
                              "from_atlas": "everything that chooses clients and windows",
                              "from_kitchen": ["clienti.q2_id", "grandi", "caricamento", "server"]}
        d = os.path.join(out_dir, s)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "risultati_p1.json"), "w") as f:
            json.dump(m, f, indent=1)
        log = os.path.join(atlas_dir, s, "risultati_p1.json.usati.log")
        if os.path.exists(log):
            shutil.copy(log, os.path.join(d, "risultati_p1.json.usati.log"))
        else:
            open(os.path.join(d, "risultati_p1.json.usati.log"), "w").close()
        report[s] = m["kitchen_merge"]
        print(f"OK    {s:8s} {m.get('db')}: {ca['n']} clients, {len(ca['riservati'])} dormant, already read on Atlas "
              f"{len(set(m.get('usati', [])))} (+ register), fraction Atlas {100 * fa:.2f}% / ours {100 * fo:.2f}%")
    json.dump({"states": report, "stops": stops}, open(os.path.join(out_dir, "merge-report.json"), "w"), indent=1)
    for x in stops:
        print("FERMO " + x)
    print("MERGE: " + ("DONE" if not stops else f"FERMO ({len(stops)} states)"))
    sys.exit(1 if stops else 0)


if __name__ == "__main__":
    main()
