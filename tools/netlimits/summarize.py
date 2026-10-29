#!/usr/bin/env python3
"""Tables of the network-limits regression, both binaries side by side (mongo-tests/out/nl_new, nl_base), and a copy of the
raw result files into results-network-limits/regression/<label>/. usage: summarize.py > tables.md"""
import collections, glob, json, os, re, shutil

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LABELS = ("base", "new")


def out(label):
    return os.path.join(ROOT, "mongo-tests", "out", f"nl_{label}")


def suites(label):
    rows = {}
    for d in ("py", "node"):
        c = collections.defaultdict(lambda: [0, 0])
        names = set()
        for x in sorted(glob.glob(os.path.join(out(label), d + "_[0-9][0-9][0-9].json"))):
            for r in json.load(open(x)):
                c[r["category"]][1] += 1
                names.add(r["name"])
                c[r["category"]][0] += r["status"] == "pass"
        rows[d] = (c, len(names))
    return rows


def pytest_totals(label, prefix):
    res = {}
    for f in sorted(glob.glob(os.path.join(out(label), prefix + "_test_*.txt"))):
        s = open(f).read()
        last = s.strip().splitlines()[-1] if s.strip() else ""
        m = re.findall(r"(\d+) (passed|failed|errors?)", last)
        p = sum(int(n) for n, k in m if k == "passed")
        b = sum(int(n) for n, k in m if k != "passed")
        res[os.path.basename(f)[len(prefix) + 1:-4]] = (p, b)
    return res


def kill(label):
    rows = {}
    for f in sorted(glob.glob(os.path.join(out(label), "kill_*.txt"))):
        for l in open(f):
            if l.startswith("TOTAL"):
                rows[os.path.basename(f)[5:-4]] = l.strip()
    return rows


for label in LABELS:
    dst = os.path.join(ROOT, "results-network-limits", "regression", label)
    os.makedirs(dst, exist_ok=True)
    for f in glob.glob(os.path.join(out(label), "*")):
        if os.path.isfile(f) and not f.endswith((".tmp", ".part", ".part.log")):
            shutil.copy(f, dst)

S = {l: suites(l) for l in LABELS}
print("| driver | category | before (base binary) | after (network-limits binary) |\n|---|---|---|---|")
for d, name in (("py", "pymongo"), ("node", "Node")):
    for cat in ("core", "extra", "replset"):
        cell = lambda l: "%d/%d" % tuple(S[l][d][0][cat]) if cat in S[l][d][0] else "not run"
        print(f"| {name} | {cat} | {cell('base')} | {cell('new')} |")
for prefix, title in (("ctl", "ctl (normal)"), ("ctlauth", "ctl with CTL_TEST_AUTH=1")):
    B, N = pytest_totals("base", prefix), pytest_totals("new", prefix)
    print(f"\n**{title}**\n\n| file | before: passed / failed | after: passed / failed |\n|---|---|---|")
    tb, tn = [0, 0], [0, 0]
    for k in sorted(set(B) | set(N)):
        b, n = B.get(k), N.get(k)
        if b: tb[0] += b[0]; tb[1] += b[1]
        if n: tn[0] += n[0]; tn[1] += n[1]
        print(f"| {k} | {'%d / %d' % b if b else '-'} | {'%d / %d' % n if n else '-'} |")
    print(f"| **total** | **{tb[0]} / {tb[1]}** | **{tn[0]} / {tn[1]}** |")
KB, KN = kill("base"), kill("new")
print("\n| SIGKILL trial | before | after |\n|---|---|---|")
for k in sorted(set(KB) | set(KN)):
    def short(s):
        if not s:
            return "-"
        kv = dict(re.findall(r"'(acked_docs|acked_ids|lost_inserts|lost_updates|missing|stale)': (\d+)", s))
        return " ".join(s.split()[:3][1:2]) + " " + ", ".join(f"{k}={v}" for k, v in kv.items())
    print(f"| {k} | {short(KB.get(k))} | {short(KN.get(k))} |")
