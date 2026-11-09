"""Compare two JUnit XML results of the same suite.
  compare.py --count FILE              passed/failed/skipped of one run
  compare.py MONGOD.xml OURS.xml       counts of both, then every test that passes on mongod and not on ours
"""
import sys
import xml.etree.ElementTree as ET


def results(path):
    r = {}
    for tc in ET.parse(path).getroot().iter("testcase"):
        name = f"{tc.get('classname', '')}::{tc.get('name')}"
        if tc.find("skipped") is not None:
            st, msg = "skipped", (tc.find("skipped").get("message") or "")
        elif tc.find("failure") is not None or tc.find("error") is not None:
            e = tc.find("failure") if tc.find("failure") is not None else tc.find("error")
            st, msg = "failed", ((e.get("message") or "") + "\n" + (e.text or ""))
        else:
            st, msg = "passed", ""
        r[name] = (st, msg)
    return r


def counts(r):
    c = {"passed": 0, "failed": 0, "skipped": 0}
    for st, _ in r.values():
        c[st] += 1
    return f"{c['passed']} passed, {c['failed']} failed, {c['skipped']} skipped"


if sys.argv[1] == "--count":
    print(counts(results(sys.argv[2])))
    sys.exit(0)
m, o = results(sys.argv[1]), results(sys.argv[2])
print(f"mongod: {counts(m)}")
print(f"ours:   {counts(o)}")
diff = [n for n, (st, _) in m.items() if st == "passed" and o.get(n, ("missing", ""))[0] != "passed"]
better = [n for n, (st, _) in o.items() if st == "passed" and m.get(n, ("missing", ""))[0] != "passed"]
print(f"pass on mongod, not on ours: {len(diff)}")
for n in diff:
    st, msg = o.get(n, ("missing", ""))
    print(f"- {n} [{st}]\n    " + "\n    ".join(msg.strip().splitlines()[:12]))
if better:
    print(f"pass on ours, not on mongod: {len(better)}")
    for n in better:
        print(f"- {n}")
