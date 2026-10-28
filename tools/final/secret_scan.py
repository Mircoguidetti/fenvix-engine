#!/usr/bin/env python3
"""Secrets in logs: look for every password, token and key the tests use in every log and output file the verification produced
(engine logs, pytest outputs, harness logs). A secret in a file is a failure. usage: secret_scan.py [--out FILE]
The secrets are the values the tests pass in createUser/updateUser/authenticate/saslStart, the admin token, and the MinIO key pair."""
import glob, json, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SECRETS = {
    "createUser/updateUser password (test_09)": "Pw-SECRET-9f3c1d7a", "wrong password (test_09)": "Hostile-SECRET-5b2e8c4d",
    "root password (test_09, gap harness)": "rootpw", "user password uapw": "uapw", "user password crpw": "crpw", "user password vpw": "vpw",
    "e2e app password": "App-Pw-E2E-7d41c9", "e2e root password": "Root-Pw-E2E-2b8e5f", "modes password": "Modes-Pw-3c9a51", "late user password": "late-pw-1",
    "admin token": "test-token-0123456789abcdef", "S3 secret key (MinIO)": "minioadmin",
    "S3 session token (s3env test)": "given-session-token", "S3 secret key (s3env test)": "given-secret-value",
}
PATTERNS = ["mongo-tests/out/server-*.log", "mongo-tests/out/final/*", "ctl-tests/out/*.log", "loadtest/out/**/server-*.log", "mongo-tests/out/final/**/*.log"]


def main():
    out = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else None
    files = sorted({f for p in PATTERNS for f in glob.glob(os.path.join(ROOT, p), recursive=True) if os.path.isfile(f)})
    found, scanned = [], 0
    for f in files:
        # server_logs only: the tests print the marker in assertions of their own source, not in what the engine writes
        try:
            data = open(f, "rb").read()
        except OSError:
            continue
        scanned += 1
        for label, s in SECRETS.items():
            n = data.count(s.encode())
            if n:
                found.append({"file": os.path.relpath(f, ROOT), "secret": label, "occurrences": n})
    rep = {"files_scanned": scanned, "secrets_checked": len(SECRETS), "found": found}
    if out:
        json.dump(rep, open(out, "w"), indent=1)
    print(json.dumps(rep, indent=1)[:4000])
    sys.exit(1 if found else 0)


if __name__ == "__main__":
    main()
