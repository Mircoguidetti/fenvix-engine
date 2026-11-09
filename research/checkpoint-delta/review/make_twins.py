"""Scripted twins: for each real thread, a scripted thread (the owner's workload.thread_script, owner's sizes)
with exactly the same number of checkpoints. Separates message-size realism from thread-length mix.
Usage: make_twins.py DS -> real/DS_twin.jsonl"""
import json, os, random, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import workload as w
ds = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(HERE, "real", ds + "_twin.jsonl"), "w") as f:
    for i, l in enumerate(open(os.path.join(HERE, "real", ds + ".jsonl"))):
        s = json.loads(l)
        n = sum(3 + 2 * len(t["rounds"]) for t in s)
        f.write(json.dumps(w.thread_script(n, random.Random(f"{w.SEED}-twin-{ds}-{i}"))) + "\n")
