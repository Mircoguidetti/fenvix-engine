#!/usr/bin/env python3
"""Secret / identity scan of every file about to be published (results-round3/real-s3/): account id, ARNs, role
name, AWS key ids, session tokens, the root password. Redacts in place, then reports counts only (never the values).
Exit 1 if anything remains after redaction.   env: R3_ACCT, R3_ROLE_NAME (from the masked secret), R3_EXTRA_SECRETS"""
import os, re, sys
roots = sys.argv[1:] or ["results-round3/real-s3"]
lits = [v for v in (os.environ.get("R3_ACCT", ""), os.environ.get("R3_ROLE_NAME", "")) if len(v) >= 6]
lits += [v for v in os.environ.get("R3_EXTRA_SECRETS", "").split(",") if len(v) >= 8]
pats = [re.compile(rb"arn:aws:[a-z0-9-]*:[a-z0-9-]*:\d{12}:[^\s\"']*"), re.compile(rb"\b(AKIA|ASIA)[A-Z0-9]{16}\b"),
        re.compile(rb"\b\d{12}\b"), re.compile(rb"IQoJb3JpZ2luX2Vj[A-Za-z0-9/+=]{20,}")]
found = 0
for root in roots:
    for d, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(d, f)
            data = open(p, "rb").read()
            new = data
            for v in lits:
                new = new.replace(v.encode(), b"<redacted>")
            for rx in pats:
                new = rx.sub(b"<redacted>", new)
            if new != data:
                found += 1
                open(p, "wb").write(new)
                print(f"redacted: {p}")
print(f"scan: {found} file(s) redacted")
for root in roots:
    for d, _, fs in os.walk(root):
        for f in fs:
            data = open(os.path.join(d, f), "rb").read()
            if any(v.encode() in data for v in lits) or any(rx.search(data) for rx in pats[:2]):
                sys.exit("secret still present after redaction")
