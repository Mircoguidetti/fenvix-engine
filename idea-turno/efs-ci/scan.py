#!/usr/bin/env python3
"""Controllo dei segreti prima di pubblicare: account, ARN, nome del ruolo, chiavi AWS, token di sessione.
Li sostituisce con <mascherato>; esce con errore se resta qualcosa. Uso: scan.py <file o cartella>..."""
import os, re, sys
lits = [v for v in (os.environ.get("R3_ACCT", ""), os.environ.get("R3_ROLE_NAME", "")) if len(v) >= 6]
pats = [re.compile(rb"arn:aws:[a-z0-9-]*:[a-z0-9-]*:\d{12}:[^\s\"']*"), re.compile(rb"\b(AKIA|ASIA)[A-Z0-9]{16}\b"),
        re.compile(rb"\b\d{12}\b"), re.compile(rb"IQoJb3JpZ2luX2Vj[A-Za-z0-9/+=]{20,}")]
def files(roots):
    for r in roots:
        if os.path.isfile(r): yield r
        for d, _, fs in os.walk(r):
            for f in fs: yield os.path.join(d, f)
n = 0
for p in files(sys.argv[1:]):
    data = open(p, "rb").read(); new = data
    for v in lits: new = new.replace(v.encode(), b"<mascherato>")
    for rx in pats: new = rx.sub(b"<mascherato>", new)
    if new != data:
        n += 1; open(p, "wb").write(new); print(f"mascherato: {p}")
print(f"scan: {n} file mascherati")
for p in files(sys.argv[1:]):
    data = open(p, "rb").read()
    if any(v.encode() in data for v in lits) or any(rx.search(data) for rx in pats[:2]):
        sys.exit("segreto ancora presente")
