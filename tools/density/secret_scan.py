#!/usr/bin/env python3
"""Secret scan of the density work before the push.
1. Every file added or changed since origin/network-limits (gzip files decompressed) is searched for real-secret
   patterns (cloud keys, private keys, provider tokens, credentials in URLs).
2. The engine logs in results-density must not contain the test passwords or the admin token the runs used.
The fixed test values (local test passwords, the MinIO test pair, the loopback admin token) are test inputs, not secrets.
usage: secret_scan.py [--out FILE]"""
import gzip
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEST_VALUES = {"root password": "density-root-pw", "user password prefix": "-density", "admin token": "density-test-token-0123456789",
               "MinIO secret": "minioadmin"}
PATTERNS = {
    "AWS access key id": r"\b(AKIA|ASIA)[0-9A-Z]{16}\b",
    "AWS secret key assignment": r"(?i)aws_secret_access_key\s*[=:]\s*['\"]?(?!minioadmin)[A-Za-z0-9/+]{30,}",
    "private key": r"-----BEGIN (RSA |EC |OPENSSH |DSA |)?PRIVATE KEY-----",
    "certificate": r"-----BEGIN CERTIFICATE-----",
    "GitHub token": r"\b(ghp|gho|ghs|ghu|ghr)_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}",
    "Slack token": r"\bxox[abpr]-[A-Za-z0-9-]{10,}",
    "Google API key": r"\bAIza[0-9A-Za-z_-]{35}\b",
    "Anthropic/OpenAI key": r"\bsk-(ant-)?[A-Za-z0-9_-]{30,}",
    "credentials in a URL": r"\b[a-z]+://[^/\s:@'\"]+:[^/\s:@'\"{]{6,}@",
}


def read(path):
    data = open(path, "rb").read()
    if path.endswith(".gz"):
        data = gzip.decompress(data)
    return data.decode("utf8", "replace")


def main():
    out = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else None
    changed = subprocess.run(["git", "-C", ROOT, "diff", "--name-only", "origin/network-limits"], capture_output=True, text=True).stdout.split()
    untracked = subprocess.run(["git", "-C", ROOT, "ls-files", "--others", "--exclude-standard"], capture_output=True, text=True).stdout.split()
    # the scanner itself holds the patterns as text
    files = sorted({f for f in changed + untracked if os.path.isfile(os.path.join(ROOT, f)) and f != "tools/density/secret_scan.py"})
    hits, log_hits, logs = [], [], 0
    for f in files:
        txt = read(os.path.join(ROOT, f))
        for name, pat in PATTERNS.items():
            for m in re.finditer(pat, txt):
                hits.append({"file": f, "pattern": name, "match": m.group(0)[:12] + "..."})
        if "engine" in os.path.basename(f) and ".log" in f:
            logs += 1
            for name, val in TEST_VALUES.items():
                if val in txt:
                    log_hits.append({"file": f, "value": name})
    res = {"files_scanned": len(files), "engine_logs_scanned": logs, "pattern_hits": hits, "test_values_in_engine_logs": log_hits,
           "clean": not hits and not log_hits}
    s = json.dumps(res, indent=1)
    print(s)
    if out:
        open(out, "w").write(s + "\n")
    sys.exit(0 if res["clean"] else 1)


if __name__ == "__main__":
    main()
