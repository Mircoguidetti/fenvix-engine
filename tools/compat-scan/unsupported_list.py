#!/usr/bin/env python3
"""Every remaining unsupported call of the scan, app by app, grouped by feature, with file:line (results9.md).
    unsupported_list.py [--profile after] [--in results.json]"""
import argparse, json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import coverage, report  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--profile", default="after")
ap.add_argument("--in", dest="inp", default=os.path.join(HERE, "results.json"))
a = ap.parse_args()
data = json.load(open(a.inp))
total = 0
for r in data["repos"]:
    if r["status"] != "scanned":
        continue
    feats = []
    for section in ("operators", "methods", "indexes"):
        for key, c in r[section].items():
            if not c["app"]:
                continue
            st = coverage.status(key, a.profile)
            rec = section == "indexes" and key in coverage.INDEX_OPTIONS and st != report.SUPPORTED
            if st == report.NOT or rec:
                feats.append((report.display(key)[0] + (" (recorded only)" if rec else ""), key, c["app"]))
    print(f"\n### {r['name']} — {sum(f[2] for f in feats)} unsupported calls in {len(feats)} features")
    if not feats:
        print("\nnone")
    for label, key, n in sorted(feats, key=lambda f: (-f[2], f[0])):
        locs = r.get("locations", {}).get(key, [])
        total += n
        shown = ", ".join(f"`{l}`" for l in locs)
        print(f"\n- **{label}** ×{n}: {shown if locs else '(no location recorded)'}")
print(f"\nTOTAL unsupported calls: {total}")
