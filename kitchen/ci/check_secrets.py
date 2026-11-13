"""Kitchen (K2, from P1 Ireland controlla_segreti.py) · before publishing: no secret of the run in any file of the
folder, .gz included: the password of our server (CM_PW, also as part of a connection string) and no private key
("PRIVATE KEY": the key of the server certificate or of the CA). Exit 1 if one is found (nothing is published).
Usage: check_secrets.py FOLDER   (the password comes from the environment, never from arguments)"""
import gzip
import os
import sys
import urllib.parse

pw = os.environ.get("CM_PW", "")
pieces = set()
if len(pw) >= 8:
    pieces |= {pw, urllib.parse.quote(pw, safe="")}
pieces.add("PRIVATE KEY")
# step9's Atlas line (2026-11-13): the passwords inside the Atlas connection strings and the parts of the API key, when
# the environment has them (absent everywhere else: nothing changes)
import re  # noqa: E402
for e in ("ATLAS_URI", "ATLAS_TARGET_URI"):
    m = re.match(r"mongodb(\+srv)?://([^@/]+)@", os.environ.get(e, ""))
    if m and ":" in m.group(2):
        p = m.group(2).split(":", 1)[1]
        pieces |= {x for x in (p, urllib.parse.unquote(p)) if len(x) >= 8}
pieces |= {x for x in os.environ.get("ATLAS_API_KEY", "").split(":") if len(x) >= 8}
found = []
for root, _, names in os.walk(sys.argv[1]):
    for n in names:
        p = os.path.join(root, n)
        try:
            data = gzip.open(p).read() if n.endswith(".gz") else open(p, "rb").read()
        except Exception:
            data = open(p, "rb").read()
        if any(x.encode() in data for x in pieces):
            found.append(os.path.relpath(p, sys.argv[1]))
if found:
    print(f"SECRET FOUND in {len(found)} files: {', '.join(found[:20])}. Not publishing.")
    sys.exit(1)
print(f"secrets check: no secret in the files ({len(pieces)} pieces searched, password {'included' if len(pw) >= 8 else 'MISSING'})")
