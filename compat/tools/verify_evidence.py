#!/usr/bin/env python3
"""Quality gate, part 3: every test/probe cited as passing evidence in inventory.csv must exist in the stored run results and must have passed there;
every cited failing one must have failed. Also checks that no 'covered' row cites only a probe/test that is missing from the results. Exits non-zero on error."""
import csv, json, os, re, sys
HERE = os.path.dirname(os.path.abspath(__file__)); C = os.path.join(HERE, "..")
rows = list(csv.DictReader(open(os.path.join(C, "inventory.csv"))))
res = {}
for tag, f in (("py", "py_main.json"), ("py", "py_retry.json"), ("node", "node_core.json"), ("node", "node_replset.json"), ("node", "node_retry.json")):
    for x in json.load(open(os.path.join(C, "results", f))):
        res.setdefault(tag, {})[x["name"]] = x
probes = {}
for f in ("probe_diff_results.json", "probe_node_results.json"):
    probes.update(json.load(open(os.path.join(C, f)))["cases"])
errs, n = [], 0
for r in rows:
    ev = r["evidence"]
    for m in re.finditer(r"passing differential tests: ([^;]*)", ev):
        for name, harn in re.findall(r"([A-Za-z0-9_.\-]+) \[([a-z+]+)\]", m.group(1)):
            n += 1
            for h in harn.split("+"):
                if res.get(h, {}).get(name, {}).get("status") != "pass":
                    errs.append(f"{r['id']}: cites {name} as passing in {h} but results say {res.get(h, {}).get(name, {}).get('status')}")
    for m in re.finditer(r"passing audit probes \(compat/probes\): ([^;]*)", ev):
        for name in re.findall(r"(?:compat|node)\.[A-Za-z0-9_.@=\[\]\-]+", m.group(1)):
            n += 1
            if probes.get(name, {}).get("status") != "pass":
                errs.append(f"{r['id']}: cites probe {name} as passing but results say {probes.get(name, {}).get('status')}")
    for m in re.finditer(r"failing audit probes \(compat/probes\): ([^;]*)", ev):
        for name in re.findall(r"(?:compat|node)\.[A-Za-z0-9_.@=\[\]\-]+", m.group(1)):
            n += 1
            if probes.get(name, {}).get("status") != "fail":
                errs.append(f"{r['id']}: cites probe {name} as failing but results say {probes.get(name, {}).get('status')}")
print(f"{n} citations checked, {len(errs)} errors")
for e in errs[:40]: print(e)
sys.exit(1 if errs else 0)
