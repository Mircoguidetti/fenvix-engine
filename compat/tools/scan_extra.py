#!/usr/bin/env python3
"""Extra usage scan for the gap ranking. Same 10 repositories, same file selection and test/app split as tools/compat-scan/scan.py (this script imports
its constants), but with detectors for features the original scan did not measure (authentication, collation, capped collections, validators, tailable
cursors, $natural, server diagnostics, user management, mapReduce, multi-index createIndexes, ...). One shallow clone at a time, deleted after the scan.
Plain text matching: counts are over- and under-estimates in the same ways as the original scan (see compat-scan.md section 2).
Writes compat/scan_extra.json: {repo: {feature: {"app": n, "test": n, "app_files": [first 3]}}}.   Usage: scan_extra.py [--tmp DIR]"""
import argparse, collections, json, os, re, shutil, subprocess, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "tools", "compat-scan"))
import scan as S   # noqa: E402  (reuses REPOS, PRUNE_DIRS, SCAN_EXT, TEST_PATH, MONGO_HINT, find_dependencies, is_minified)

CONFIG_EXT = {".env", ".yml", ".yaml", ".properties", ".conf", ".ini", ".toml", ".cfg"}
FEATURES = {
    "auth: credentials in a connection string or auth options": r"""mongodb(?:\+srv)?://[^/\s"'`<>@$]+:[^@\s"'`<>]+@|authSource|authMechanism""",
    "collation": r"""\.collation\(|\bcollation\s*[:=(]|\{\s*locale\s*:""",
    "capped collection": r"""\bcapped\s*[:=]\s*(?:true|True)""",
    "collection validation ($jsonSchema, validationLevel, validationAction)": r"""\$jsonSchema|validationLevel|validationAction""",
    "time series collection": r"""\btimeseries\s*[:=]|\btimeField\b|time_series""",
    "view (viewOn / createView)": r"""\bviewOn\b|createView\s*\(""",
    "tailable cursor": r"""\btailable\b|\bawaitData\b|CursorType\.TAILABLE|\bTAILABLE""",
    "$natural sort/hint": r"""\$natural""",
    "diagnostics commands (serverStatus, currentOp, killOp, collStats, dbStats, hostInfo, db.stats())": r"""\{\s*(?:serverStatus|currentOp|killOp|collStats|dbStats|hostInfo|getCmdLineOpts|connPoolStats)\s*:|\.(?:serverStatus|currentOp|killOp|hostInfo)\s*\(|command\(\s*["'](?:serverStatus|dbStats|collStats|currentOp)["']|\bdb\.stats\s*\(""",
    "user/role management commands": r"""db\.(?:createUser|dropUser|updateUser)\s*\(|\{\s*(?:createUser|dropUser|usersInfo|updateUser|grantRolesToUser|createRole)\s*:|command\(\s*["'](?:createUser|dropUser|usersInfo|grantRolesToUser|createRole)["']""",
    "mapReduce": r"""\bmapReduce\b|\bmap_reduce\b""",
    "createIndexes with an array of indexes (createIndexes([...]) / create_indexes([...]) / IndexModel)": r"""\.createIndexes\s*\(\s*\[|\bcreate_indexes\s*\(\s*\[|\bIndexModel\b""",
    "profiler (setProfilingLevel / profilingLevel)": r"""setProfilingLevel|profilingLevel""",
    "positional projection (field.$ : 1)": r"""["']\w[\w.]*\.\$["']\s*:\s*1""",
    "$elemMatch (query or projection; scan cannot tell)": r"""\$elemMatch""",
    "change stream pre/post images and expanded events": r"""fullDocumentBeforeChange|showExpandedEvents|startAtOperationTime|allChangesForCluster|changeStreamPreAndPostImages""",
    "collMod": r"""\bcollMod\b""",
    "reIndex": r"""\breIndex\s*\(""",
    "returnKey / showRecordId": r"""\.returnKey\(|\.showRecordId\(|\bshowRecordId\b|\breturnKey\b""",
    "planCache commands": r"""planCacheClear|planCacheListFilters|planCacheSetFilter""",
    "X.509 / AWS / OIDC / Kerberos auth mechanisms": r"""MONGODB-X509|MONGODB-AWS|MONGODB-OIDC|GSSAPI|authMechanism\s*[:=]""",
    "compression option (compressors=)": r"""compressors\s*[=:]|\bcompressor""",
    "TLS options (tls=true, ssl=true, tlsCAFile, ...)": r"""\btls\s*[=:]\s*(?:true|True)|\bssl\s*[=:]\s*(?:true|True)|tlsCAFile|tlsCertificateKeyFile|[?&]tls=true|[?&]ssl=true""",
    "retryable writes (retryWrites mentioned; the drivers enable it by default)": r"""retryWrites|retry_writes""",
}
RX = {k: re.compile(v) for k, v in FEATURES.items()}


def scan(root):
    out = {k: {"app": 0, "test": 0, "app_files": []} for k in FEATURES}
    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if d not in S.PRUNE_DIRS]
        for fn in fns:
            if fn in S.LOCKFILES:
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext not in S.SCAN_EXT and ext not in CONFIG_EXT and not fn.startswith(".env"):
                continue
            path = os.path.join(dp, fn)
            rel = os.path.relpath(path, root).replace(os.sep, "/")
            try:
                if os.path.getsize(path) > S.MAX_FILE_BYTES:
                    continue
                text = open(path, errors="ignore").read()
            except OSError:
                continue
            if S.is_minified(fn, text[:20000]):
                continue
            kind = "test" if S.TEST_PATH.search(rel) else "app"
            hinted = bool(S.MONGO_HINT.search(text)) or ext in CONFIG_EXT or fn.startswith(".env")
            if not hinted:
                continue
            for k, rx in RX.items():
                n = len(rx.findall(text))
                if n:
                    out[k][kind] += n
                    if kind == "app" and len(out[k]["app_files"]) < 3:
                        out[k]["app_files"].append(rel)
    return out


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--tmp", default=tempfile.gettempdir()); ap.add_argument("--only", default="")
    a = ap.parse_args()
    res = {}
    for url in S.REPOS:
        name = url.rstrip("/").split("/")[-1]
        if a.only and a.only not in name:
            continue
        dest = os.path.join(a.tmp, "scanx_" + name)
        shutil.rmtree(dest, ignore_errors=True)
        r = subprocess.run(["git", "clone", "--depth", "1", "--quiet", url, dest], capture_output=True, text=True, timeout=1200)
        if r.returncode != 0:
            res[name] = {"error": r.stderr.strip()[-200:]}; print(name, "clone failed"); continue
        commit = subprocess.run(["git", "-C", dest, "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
        try:
            res[name] = {"commit": commit, "features": scan(dest)}
            print(name, commit, sum(1 for v in res[name]["features"].values() if v["app"]), "features with app hits", flush=True)
        finally:
            shutil.rmtree(dest, ignore_errors=True)
    json.dump(res, open(os.path.join(HERE, "..", "scan_extra.json" if not a.only else "scan_extra_partial.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
