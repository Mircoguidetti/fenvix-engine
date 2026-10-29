#!/usr/bin/env python3
"""(main-candidate copy of tools/round2/secret_scan.py: base origin/engine-round2, values of test_19 (fake IMDS / fake S3) added,
and two more repository patterns, AWS ARNs and 12-digit AWS account ids, because results-aws-test3* come from a run on real AWS.)
Secret scan of the network-limits work, two parts:
1. logs and outputs: every password, nonce marker and token the tests send must appear in no engine log and no test output
   (the same check as tools/final/secret_scan.py, with the values of test_13 / test_14 added);
2. repository files: every file added or changed since origin/final-engine is searched for real-secret patterns (cloud keys,
   private keys, provider tokens, credentials in URLs). The fixed test values (fake passwords such as "Limit-SECRET-...", the
   MinIO test pair, the test admin token) are listed apart: they are test inputs, not secrets.
usage: secret_scan.py [--out FILE]"""
import glob, json, os, re, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SECRETS = {
    "test_09 password": "Pw-SECRET-9f3c1d7a", "test_09 hostile password": "Hostile-SECRET-5b2e8c4d",
    "test_13 user password": "Allow-SECRET-7c41e9b2", "test_13 logged password": "Logged-SECRET-31d9a7", "test_13 client nonce": "NONCE-MARKER-4b1c9e2f7a",
    "test_14 user password": "Limit-SECRET-2e8d4f6a", "test_14 wrong password": "Wrong-SECRET-9a7b3c1d",
    "test_15 password": "Round2-SECRET-5b1e7c9d", "test_17 password": "Grant-SECRET-71c3a9e2", "test_17 hostile root password": "Taken-SECRET-0000",
    "root password": "rootpw", "e2e app password": "App-Pw-E2E-7d41c9", "e2e root password": "Root-Pw-E2E-2b8e5f", "modes password": "Modes-Pw-3c9a51",
    "admin token": "test-token-0123456789abcdef", "S3 secret key (MinIO)": "minioadmin",
    "S3 session token (s3env test)": "given-session-token", "S3 secret key (s3env test)": "given-secret-value",
    "test_19 fake IMDS secret keys": "fake-secret-", "test_19 fake IMDS session tokens": "fake-session-token-",
    "test_19 fake static secret": "fake-static-secret-", "test_19 fake static token": "fake-static-token",
    "test_19 fake IMDS session token (metadata)": "imds-session-",
}
LOGS = ["mongo-tests/out/server-*.log", "mongo-tests/out/nl_mc/*", "ctl-tests/out/*.log", "ctl-tests/out/*.txt", "loadtest/out/**/*.log",
        "results-main-candidate/**/*.txt"]
# pytest outputs print the test's own source lines on a failure (which hold the test values); engine logs never may
ENGINE_LOG = re.compile(r"(ctl-tests/out/.*\.log|server-\d+\.log)$")
PATTERNS = {
    "AWS access key id": r"\b(AKIA|ASIA)[0-9A-Z]{16}\b",
    "AWS secret key assignment": r"(?i)aws_secret_access_key\s*[=:]\s*['\"]?(?!minioadmin|given-secret-value)[A-Za-z0-9/+]{30,}",
    "private key": r"-----BEGIN (RSA |EC |OPENSSH |DSA |)PRIVATE KEY-----",
    "GitHub token": r"\b(ghp|gho|ghs|ghu|ghr)_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}",
    "Slack token": r"\bxox[abpr]-[A-Za-z0-9-]{10,}",
    "Google API key": r"\bAIza[0-9A-Za-z_-]{35}\b",
    "Anthropic/OpenAI key": r"\bsk-(ant-)?[A-Za-z0-9_-]{30,}",
    "credentials in a URL": r"\b[a-z]+://[^/\s:@'\"]+:(?!rootpw@|minioadmin@|pw@|\{)[^/\s:@'\"]{6,}@",
    "AWS ARN with an account id": r"\barn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:\d{12}:",
    "AWS account id after 'account'": r"(?i)account[^\n]{0,20}?\b\d{12}\b",
}


def main():
    out = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else None
    files = sorted({f for p in LOGS for f in glob.glob(os.path.join(ROOT, p), recursive=True) if os.path.isfile(f)})
    log_hits, scanned = [], 0
    for f in files:
        data = open(f, "rb").read()
        scanned += 1
        rel = os.path.relpath(f, ROOT)
        for label, s in SECRETS.items():
            n = data.count(s.encode())
            if n:
                log_hits.append({"file": rel, "secret": label, "occurrences": n, "engine_log": bool(ENGINE_LOG.search(rel))})
    changed = subprocess.run("git diff --name-only origin/engine-round2; git ls-files --others --exclude-standard", shell=True, cwd=ROOT,
                             capture_output=True, text=True).stdout.split()
    repo_hits, test_values, repo_scanned = [], [], 0
    for rel in sorted(set(changed)):
        p = os.path.join(ROOT, rel)
        if not os.path.isfile(p):
            continue
        repo_scanned += 1
        text = open(p, "rb").read().decode("utf8", "replace")
        for label, rx in PATTERNS.items():
            for m in re.finditer(rx, text):
                repo_hits.append({"file": rel, "pattern": label, "line": text.count("\n", 0, m.start()) + 1})
        for label, s in SECRETS.items():
            if s in text:
                test_values.append({"file": rel, "test_value": label})
    rep = {"log_files_scanned": scanned, "secrets_checked": len(SECRETS), "found_in_logs": log_hits,
           "found_in_engine_logs": [h for h in log_hits if h["engine_log"]],
           "repo_files_scanned": repo_scanned, "repo_secret_patterns_found": repo_hits, "fixed_test_values_in_repo_files": test_values}
    if out:
        json.dump(rep, open(out, "w"), indent=1)
    print(json.dumps({k: v for k, v in rep.items() if k != "fixed_test_values_in_repo_files"}, indent=1)[:6000])
    sys.exit(1 if rep["found_in_engine_logs"] or repo_hits else 0)


if __name__ == "__main__":
    main()
