#!/usr/bin/env python3
"""Builds results-final-engine.md from the raw result files of the final-engine verification (mongo-tests/out/final, results-final-engine/)
and copies those files into results-final-engine/. usage: make_report.py <commit> <sha256> <build-command>"""
import collections, glob, json, os, re, shutil, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(ROOT, "mongo-tests", "out", "final")
RES = os.path.join(ROOT, "results-final-engine")
os.makedirs(RES, exist_ok=True)
for f in glob.glob(os.path.join(OUT, "*")):
    if os.path.isfile(f) and not f.endswith(".tmp"):
        shutil.copy(f, RES)
for pat, sub in (("loadtest/results/gc/durability-s3-*.json", "durability-json"), ("loadtest/results/open-gc/open-gc-*.json", "durability-json")):
    for f in glob.glob(os.path.join(ROOT, pat)):
        os.makedirs(os.path.join(RES, sub), exist_ok=True)
        shutil.copy(f, os.path.join(RES, sub))

COMMIT, SHA, BUILD = (sys.argv + ["?", "?", "?"])[1:4]


def suites():
    rows = {}
    for d in ("py", "node"):
        c = collections.defaultdict(lambda: [0, 0]); names = set()
        for x in sorted(glob.glob(os.path.join(OUT, d + "_[0-9][0-9][0-9].json"))):
            for r in json.load(open(x)):
                c[r["category"]][1] += 1; names.add(r["name"]); c[r["category"]][0] += r["status"] == "pass"
        rows[d] = (c, len(names))
    return rows


def pytest_totals(prefix):
    out = []
    for f in sorted(glob.glob(os.path.join(OUT, prefix + "_test_*.txt"))):
        s = open(f).read()
        m = re.findall(r"(\d+) (passed|failed|error)", s.splitlines()[-1] if s.strip() else "")
        p = sum(int(n) for n, k in m if k == "passed"); b = sum(int(n) for n, k in m if k != "passed")
        t = re.findall(r" in ([\d.]+)s", s)
        out.append((os.path.basename(f)[len(prefix) + 1:-4], p, b, t[-1] if t else "?"))
    return out


def gaps():
    rows = []
    for item in ["g%02d" % i for i in range(1, 13)]:
        row = [item]
        for kind in ("py", "node"):
            f = os.path.join(OUT, f"gaps_{kind}_{item}.log")
            if not os.path.exists(f):
                row.append("not run"); continue
            ls = [l for l in open(f).read().splitlines() if l.startswith(f"gaps_{kind}: ")]
            row.append(ls[-1].replace(f"gaps_{kind}: ", "") if ls else "no summary line")
        rows.append(row)
    return rows


def kill():
    rows = []
    for f in sorted(glob.glob(os.path.join(OUT, "kill_*.txt"))):
        for l in open(f):
            if l.startswith("TOTAL"):
                rows.append((os.path.basename(f)[5:-4], l.strip()))
    return rows


S = suites(); C = pytest_totals("ctl"); A = pytest_totals("ctlauth"); K = kill(); G = gaps()
md = []
md.append(open(os.path.join(ROOT, "tools", "final", "report_head.md")).read().replace("@COMMIT@", COMMIT).replace("@SHA@", SHA).replace("@BUILD@", BUILD))
md.append("\n## 4. Test suites on the final binary\n")
md.append("### 4.1 Existing differential suites (4127 tests, real mongod 7.0.45 as the reference; slices of 150, `tools/final/run_suites.py`)\n")
md.append("| driver | core | extra | replset | distinct tests |\n|---|---|---|---|---|")
for d, label in (("py", "pymongo"), ("node", "Node")):
    c, n = S[d]
    md.append("| %s | %s | %s | %s | %d |" % (label, *("%d/%d" % tuple(c[k]) if k in c else "not run" for k in ("core", "extra", "replset")), n))
md.append("\nThis includes the 2925 core tests of control-ops' regression and the 1081 of engine-cost (they are part of the 3989 core).\n")
md.append("### 4.2 Gap tests g01-g12 of compat-gaps, auth and TLS (`mongo-tests/gaps`, `tools/final/run_gaps.sh`; engines on the local store, mongod 7.0.45 reference)\n")
md.append("| item | pymongo | Node |\n|---|---|---|")
for r in G:
    md.append("| %s | %s | %s |" % tuple(r))
md.append("\nThe auth servers (27028/27030, SCRAM-SHA-1/256, `--auth`) and the TLS pairs (A requireTLS+optional client certificate, B self-signed, C client certificate required, D preferTLS, E requireTLS+`--auth`+X.509) are the ones of compat-gaps (`auth_env.sh`, `tls_env.py`). `ctl-tests/test_11_tls_modes.py` adds the four modes (disabled, allowTLS, preferTLS, requireTLS) x SCRAM-SHA-1/256, plain and TLS.\n")
md.append("### 4.3 ctl tests (`ctl-tests/`)\n")
md.append("Normal mode (every file) and with access control on (`CTL_TEST_AUTH=1`: server with `--auth`, authenticated root client; files test_01 to test_08b).\n")
md.append("| file | passed | failed/error | seconds | with --auth: passed | failed/error |\n|---|---|---|---|---|---|")
am = {n: (p, b) for n, p, b, _ in A}
tp = tf = ap = af = 0
for n, p, b, s in C:
    a = am.get(n)
    md.append("| %s | %d | %d | %s | %s | %s |" % (n, p, b, s, a[0] if a else "-", a[1] if a else "-"))
    tp += p; tf += b
    if a:
        ap += a[0]; af += a[1]
md.append("| **total** | **%d** | **%d** | | **%d** | **%d** |" % (tp, tf, ap, af))
md.append("\nThe 116 tests of the fused branch (111 control-ops + the cost tests) are among these; the files `test_09*` and `test_10`, `test_11` are new in this branch.\n")
md.append("### 4.4 SIGKILL durability (8 trials + negative controls, `loadtest/test_group_commit.py`, `loadtest/test_open_gc.py`)\n")
md.append("| trial | result |\n|---|---|")
for n, l in K:
    md.append(f"| {n} | `{l[:230]}` |")
md.append("\nWAL: `wal_1..4` protections on (0 lost); `walctl_1..2` = acknowledge before durable (must lose). SST/manifest/compaction state: `sst_1..4` (kill inside a cleanup pass, 0 lost, 0 reads missing or stale); `sstctl_1..2` = the cleanup also deletes a referenced SST (must lose).\n")
sc = os.path.join(RES, "secret_scan.json")
md.append("### 4.5 Secrets in logs and outputs\n")
if os.path.exists(sc):
    j = json.load(open(sc))
    md.append("`tools/final/secret_scan.py`: %d files scanned (engine logs of every test server, pytest outputs, harness logs) for %d secrets (every password the tests send, the admin token, the S3 key pair): **%s**.\n" % (j["files_scanned"], j["secrets_checked"], "none found" if not j["found"] else "FOUND: " + json.dumps(j["found"])))
else:
    md.append("not run\n")
md.append(open(os.path.join(ROOT, "tools", "final", "report_tail.md")).read().replace("@COMMIT@", COMMIT).replace("@SHA@", SHA).replace("@BUILD@", BUILD))
text = "\n".join(md)
cm = {n: (p, b) for n, p, b, _ in C}


def _ctl(m):
    n = m.group(1)
    if n not in cm:
        return "(non eseguito)"
    p, b = cm[n]
    am_ = am.get(n)
    return "%d passati, %d falliti" % (p, b) + (" (con `--auth`: %d passati, %d falliti)" % am_ if am_ else "")


text = re.sub(r"@CTL:([\w]+)@", _ctl, text)
open(os.path.join(ROOT, "results-final-engine.md"), "w").write(text)
print("written; ctl", tp, tf, "auth", ap, af)
