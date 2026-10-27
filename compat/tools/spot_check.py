#!/usr/bin/env python3
"""Quality gate, part 2: pick N random inventory rows (seeded) and print, from the page behind docs_url, the page title and the passage that
mentions the row, so a human can confirm name/category/page agree. Usage: spot_check.py [--n 30] [--seed 7]"""
import argparse, csv, os, random, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fetch import fetch
HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser(); ap.add_argument("--n", type=int, default=30); ap.add_argument("--seed", type=int, default=7)
a = ap.parse_args()
rows = list(csv.DictReader(open(os.path.join(HERE, "..", "inventory_base.csv"))))
random.seed(a.seed)
for i, r in enumerate(random.sample(rows, a.n), 1):
    t = fetch(r["docs_url"]) or ""
    title = (re.search(r"^# (.+)$", t, re.M) or re.search(r"<title>([^<]+)", t) or [None, "?"])[1][:80]
    k = r["_kind"]
    if k.startswith("anchor:"):
        m = re.search(r'id="' + re.escape(k[7:]) + r'"[^>]*>(.{0,200})', t, re.S)
        ctx = re.sub(r"<[^>]+>", "", m.group(1))[:140].strip() if m else "ANCHOR MISSING"
    elif k == "index":
        ctx = "(command listed by listCommands only; docs_url is the command index)"
    else:
        j = t.find(r["_check"])
        ctx = re.sub(r"\s+", " ", t[max(0, j - 60): j + 160]) if j >= 0 else "NAME NOT ON PAGE"
    print(f"{i:2}. {r['id']}  [{r['category']}]\n    url: {r['docs_url']}\n    page: {title}\n    ctx: {ctx}\n")
