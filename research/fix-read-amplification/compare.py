"""Compare the answers of ab_answers.py runs, stage by stage and query by query.
Usage: compare.py A.json B.json [C.json ...]   (the first is the reference; prints every difference, exit 1 on any)"""
import json
import sys

runs = [json.load(open(p)) for p in sys.argv[1:]]
ref = runs[0]
bad = 0
total = 0
for r, path in zip(runs[1:], sys.argv[2:]):
    for sa, sb in zip(ref["stages"], r["stages"]):
        for q, va in sa["answers"].items():
            total += 1
            vb = sb["answers"].get(q)
            if va != vb:
                bad += 1
                print(f"DIFFERENT {path} stage '{sa['stage']}' {q}: {va} against {vb}")
    if len(ref["stages"]) != len(r["stages"]):
        bad += 1
        print(f"DIFFERENT {path}: {len(r['stages'])} stages against {len(ref['stages'])}")
print(f"{total} answers compared, {bad} different")
sys.exit(1 if bad else 0)
