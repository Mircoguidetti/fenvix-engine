"""Compare the end-to-end answers of mongod and ours: compare_e2e.py results/mongod/e2e-X.json results/ours/e2e-X.json
Prints the number of users compared and every user (or extra key) whose answers differ."""
import json
import sys

m, o = (json.load(open(p)) for p in sys.argv[1:3])
ma, oa = m["answers"], o["answers"]
keys = sorted(set(ma) | set(oa))
diff = [k for k in keys if ma.get(k) != oa.get(k)]
users = [k for k in keys if not k.startswith("_")]
print(f"{m['adapter']}: {len(users)} users x {m['turns']} turns; differences: {len(diff)}"
      f" (mongod {m['seconds']} s, ours {o['seconds']} s)")
for k in diff:
    print(f"  {k}:\n    mongod: {json.dumps(ma.get(k))[:300]}\n    ours:   {json.dumps(oa.get(k))[:300]}")
