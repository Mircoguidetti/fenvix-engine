"""Attribute each WAITS_GET (u = unix ms of its start) and each WAITS command line to the diag op windows [u0, u1]."""
import re, sys
ops = []
for l in open(sys.argv[1]):
    m = re.search(r"u0=([\d.]+) u1=([\d.]+) (.{28})", l)
    if m: ops.append((float(m.group(1)), float(m.group(2)), m.group(3).strip()))
want = set(sys.argv[3].split(",")) if len(sys.argv) > 3 else None
for l in open(sys.argv[2]):
    if not (l.startswith("WAITS_GET") or l.startswith("WAITS ")): continue
    u = float(re.search(r" u=([\d.]+)", l).group(1))
    for a, b, name in ops:
        if a <= u <= b and (want is None or any(w in name for w in want)):
            print(f"{name:14s} {l.strip()[:230]}")
