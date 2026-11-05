"""Review of K1: reads the raw phase files of prova_infinite.py (one giro folder) and reports, per phase, the profiler
entries of the script's finds: how many, how many warm ("caldo") finds report bytes read from the store, how many
entries are marked fenvixS3Shared, and per query and temperature the bytesRead p50 / max.
Usage: python3 rev_k1_profile_check.py GIRO_FOLDER [GIRO_FOLDER...]
"""
import glob
import json
import os
import statistics
import sys


def entries(d):
    out = list(d.get("profiler") or [])
    for lv in d.get("livelli") or []:
        out += list(lv.get("profiler") or [])
    return out


def main():
    for folder in sys.argv[1:]:
        for f in sorted(glob.glob(os.path.join(folder, "fase_*.json"))):
            d = json.load(open(f))
            prof = [p for p in entries(d) if isinstance(p.get("command"), dict) and
                    str(p["command"].get("comment", "")).startswith("p1|")]
            if not prof:
                continue
            groups = {}
            shared = 0
            for p in prof:
                parts = p["command"]["comment"].split("|")
                temp = "warm" if "caldo" in parts[-1] else "cold"
                q = parts[3] if parts[1] == "par" else parts[1]
                lvl = parts[2] if parts[1] == "par" else "-"
                b = ((p.get("storage") or {}).get("data") or {}).get("bytesRead")
                groups.setdefault((lvl, q, temp), []).append(b or 0)
                shared += 1 if p.get("fenvixS3Shared") else 0
            print(f"{os.path.basename(folder)}/{os.path.basename(f)}: {len(prof)} entries, fenvixS3Shared {shared}")
            for (lvl, q, temp), bs in sorted(groups.items()):
                nz = sum(1 for b in bs if b)
                print(f"   level {lvl:>3} {q:4s} {temp}: n {len(bs)}, bytes>0 {nz}, bytes p50 {statistics.median(bs):.0f} "
                      f"max {max(bs)}")


if __name__ == "__main__":
    main()
