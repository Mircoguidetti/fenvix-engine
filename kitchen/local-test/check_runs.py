"""Kitchen local test (K2b) · the checks after the runs, from the files the runs left (nothing else):
  1. load: the fraction each database reached, against the fake Atlas (same part of the fake year);
  2. one to one: in every size the same clients, phase by phase, and the same as the fake Atlas plan (plans.json);
  3. our data: every Q2 (one order by _id) found its order (our _ids, not the Atlas ones);
  4. C1: cold minus warm per size, p50 (the first size must not start warm);
  5. C6: SST count at the start and at the end of every cook;
  6. point 4: S3 counters of every cook, and the SPESA lines with S3 from them.
Usage: check_runs.py WORK ATLAS_STATES OUT_DIR [OUT_DIR ...]   (folders relative to WORK)"""
import glob
import json
import os
import re
import statistics
import sys

W, atlas = sys.argv[1], sys.argv[2]
outs = sys.argv[3:]
GB = 1 << 30
problems = []


def p50(xs):
    return round(statistics.median(xs), 2) if xs else None


def measures(size_dir):
    """giro name, phase -> list of measures (from the script's phase files)."""
    out = {}
    for f in glob.glob(os.path.join(size_dir, "dati", "*", "giro_finale_*", "fase_*.json")):
        try:
            d = json.load(open(f))
        except Exception:
            continue
        for m in d.get("misure") or []:
            if isinstance(m, dict) and "freddo" in m:
                out.setdefault((d.get("giro"), d.get("fase")), []).append(m)
    return out


print("1. load (fraction reached, fake year up to):")
for o in outs:
    s = os.path.join(W, o, "client", "load", "load-summary.json")
    if not os.path.exists(s):
        continue
    for r in json.load(open(s))["states"]:
        c = next(x for x in r["steps"] if x["cmd"] == "carica")
        a = json.load(open(os.path.join(W, atlas, r["state"], "risultati_p1.json")))
        fa = a["caricamento"]["byte_bson"] / (a["config"]["gb"] * GB)
        ok = abs(c["reached_fraction"] - min(fa, 1.0)) <= 0.005 or (fa >= 1 and c["reached_fraction"] >= 1)
        problems += [] if ok else [f"load {r['state']}: {c['reached_fraction']} vs Atlas {fa}"]
        print(f"   {'OK  ' if ok else 'DIFF'} {o} {r['state']:8s}: ours {100 * c['reached_fraction']:.2f}% up to "
              f"{c['end_day']}, Atlas {100 * fa:.2f}% up to {r['atlas_end_day']}; 2 KB "
              f"{[x.get('2kb_done') for x in r['steps'] if x['cmd'] == 'giro']}")
print("2. one to one (clients per phase, every size and the fake Atlas plan):")
sizes = {}
for o in outs:
    for z in sorted(glob.glob(os.path.join(W, o, "client", "size-*"))):
        sizes[f"{o}/{os.path.basename(z)}"] = measures(z)
ref_name = next(iter(sizes), None)
for name, ms in sizes.items():
    same = {k: sorted(m["cliente"] or "" for m in v) for k, v in ms.items()}
    ref = {k: sorted(m["cliente"] or "" for m in v) for k, v in sizes[ref_name].items()}
    ok = same == ref
    problems += [] if ok else [f"{name}: clients differ from {ref_name}"]
    n = sum(len(v) for v in ms.values())
    print(f"   {'OK  ' if ok else 'DIFF'} {name}: {len(ms)} phases with single reads, {n} reads, same clients as {ref_name}: {ok}")
for o in outs:
    pj = os.path.join(W, o, "client", "plans", "plans.json")
    if os.path.exists(pj):
        p = json.load(open(pj))
        print(f"   {o}: plans.json: {len(p['steps'])} steps, different {len(p['different'])}, FERMO {len(p['fermo_kitchen'])}")
        problems += p["different"] + p["fermo_kitchen"]
# the clients the sizes read = the plan's clients (single reads only: the parallel steps are in other files)
if ref_name:
    pj = os.path.join(W, outs[0], "client", "plans", "plans.json")
    plan_clients = set()
    for s in json.load(open(pj))["steps"]:
        plan_clients |= set(s["atlas_clients"])
    read = {int(m["i"]) for v in sizes[ref_name].values() for m in v if m.get("i") is not None}
    extra = read - plan_clients
    print(f"   clients read in single reads by {ref_name}: {len(read)}, outside the fake Atlas plan: {len(extra)}")
    problems += [f"{len(extra)} clients outside the plan"] if extra else []
print("3. Q2 on our data (one order by _id):")
for name, ms in sizes.items():
    q2 = [m for (g, f), v in ms.items() if f == "Q2" for m in v]
    found = sum(1 for m in q2 if (m.get("freddo") or {}).get("doc") == 1)
    ok = q2 and found == len(q2)
    problems += [] if ok else [f"{name}: Q2 found {found} of {len(q2)}"]
    print(f"   {'OK  ' if ok else 'DIFF'} {name}: {found} of {len(q2)} Q2 found their order")
print("4. C1, cold minus warm, p50 in ms (first giro of each size, m30_singoli; local times say nothing about AWS):")
for name, ms in sizes.items():
    row = []
    for q in ("Q2", "Q3", "Q1"):
        v = ms.get(("m30_singoli", q)) or []
        d = [m["freddo"]["ms"] - m["caldo"][0]["ms"] for m in v if m.get("freddo") and m.get("caldo")]
        row.append(f"{q} {p50(d)}")
    print(f"   {name}: " + ", ".join(row))
print("5. C6, SSTs in the pantry at the start and at the end of every cook:")
for o in outs:
    for s in sorted(glob.glob(os.path.join(W, o, "server-*"))):
        a = os.path.join(s, "sst-start.json")
        b = os.path.join(s, "sst-end.json")
        f = lambda p: (json.load(open(p))["total"] if os.path.exists(p) else None)
        ta, tb = f(a), f(b)
        if ta is None or tb is None:
            problems.append(f"{o}/{os.path.basename(s)}: SST count missing")
        print(f"   {o}/{os.path.basename(s)}: start {ta and ta['sst']} SST ({ta and round(ta['sst_bytes'] / 2**20, 1)} MiB), "
              f"end {tb and tb['sst']} SST ({tb and round(tb['sst_bytes'] / 2**20, 1)} MiB)")
print("6. point 4, S3 counters of every cook and the SPESA line:")
for o in outs:
    for s in sorted(glob.glob(os.path.join(W, o, "server-*"))):
        p = os.path.join(s, "s3-counters.json")
        c = json.load(open(p)) if os.path.exists(p) else {}
        if not c.get("available"):
            problems.append(f"{o}/{os.path.basename(s)}: no S3 counters")
        print(f"   {o}/{os.path.basename(s)}: available {c.get('available')}, billed like PUT {c.get('billed_like_put')}, "
              f"like GET {c.get('billed_like_get')}, {c.get('dollars')} $")
    n = o.replace("out-", "")
    log = os.path.join(W, f"run-{n}.log")
    if os.path.exists(log):
        sp = [r for r in open(log) if r.startswith("SPESA")]
        print(f"   {o}: {len(sp)} SPESA lines; last: {sp[-1].strip() if sp else 'none'}")
        if not sp or not re.search(r"S3 [0-9,]+ \$ from the cooks", sp[-1]):
            problems.append(f"{o}: SPESA line without S3")
print("CHECKS: " + ("ALL OK" if not problems else f"{len(problems)} PROBLEMS: " + "; ".join(problems[:10])))
