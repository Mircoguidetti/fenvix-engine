import json, os, sys
os.environ["S9_MODE"]="local"; os.environ["RUN_ID"]="111"
sys.path.insert(0, ".")
import decide
def r(f):
    raw=json.load(open(f"../commands/{f}"))
    return decide.resolve(raw)
m2=r("03-M2-extras-write-peak.json"); b1=r("09-B1-read-rounds-1tb.json"); w1=r("10-W1-first-next-ops-reads-after-writes-1tb.json")
mark=decide.pantry_mark(m2, 111); print("M2 mark:", mark)
print("B1 after M2:", decide.pantry_mark_refusal(b1, [mark]))
print("W1 after M2:", decide.pantry_mark_refusal(w1, [mark]))
p=r("pilot-P-small.json"); print("pilot mark:", decide.pantry_mark(p, 1))
m1=r("02-M1-m30.json"); print("M1 after M2:", decide.pantry_mark_refusal(m1,[mark]) is not None)
