"""Run the knife-edge battery (knife_cases.py) against the reference and our server and report how many answers agree.
usage: geo_knife_battery.py <ref-uri> <ours-uri> [--write-matched knife_matched.json]"""
import json, sys, pymongo
from knife_cases import OFFS, build

def run(uri):
    db = pymongo.MongoClient(uri)["probe_kb"]
    out = {}
    for (ox, oy, s) in OFFS:
        stored, queries = build(ox, oy, s)
        col = db.s; col.drop()
        names = list(stored)
        col.insert_many([dict(_id=i + 1, name=n, g=stored[n]) for i, n in enumerate(names)])
        for qn, g in queries.items():
            for op in ("$geoIntersects", "$geoWithin"):
                try: r = sorted(d["name"] for d in col.find({"g": {op: {"$geometry": g}}}))
                except Exception as e: r = "ERR %s" % getattr(e, "code", e)
                out[f"{ox},{oy},{s}|{qn}|{op}"] = r
    return out

if __name__ == "__main__":
    ref, ours = run(sys.argv[1]), run(sys.argv[2])
    bad = [k for k in ref if ref[k] != ours[k]]
    print(f"{len(ref) - len(bad)}/{len(ref)} answers agree ({len(bad)} differ)")
    for k in bad:
        r, o = ref[k], ours[k]
        print(" ", k, "ref-only", sorted(set(r) - set(o)), "ours-only", sorted(set(o) - set(r)))
    if "--write-matched" in sys.argv:
        json.dump(sorted(k for k in ref if ref[k] == ours[k]), open(sys.argv[sys.argv.index("--write-matched") + 1], "w"), indent=0)
