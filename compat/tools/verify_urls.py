#!/usr/bin/env python3
"""Check every inventory_base.csv row against its docs_url: the page must have been fetched with HTTP 200 and must
contain the row's check string (md rows) or the HTML anchor id (driver API rows). Writes compat/verify_report.txt."""
import csv, os, re, sys
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fetch import fetch
HERE = os.path.dirname(os.path.abspath(__file__))
rows = list(csv.DictReader(open(os.path.join(HERE, "..", "inventory_base.csv"))))
urls = sorted({r["docs_url"] for r in rows})
with ThreadPoolExecutor(8) as ex:
    pages = dict(zip(urls, ex.map(fetch, urls)))
bad = []
for r in rows:
    t = pages[r["docs_url"]]
    if t is None:
        bad.append((r["id"], "page not fetched (non-200)", r["docs_url"])); continue
    k = r["_kind"]
    if k == "index":      # command only present in listCommands; docs_url is the index page
        continue
    if k.startswith("anchor:"):
        if f'id="{k[7:]}"' not in t:
            bad.append((r["id"], "anchor missing", r["docs_url"]))
    elif r["_check"] not in t:
        bad.append((r["id"], "name not found on page: " + r["_check"], r["docs_url"]))
with open(os.path.join(HERE, "..", "verify_report.txt"), "w") as f:
    f.write(f"rows={len(rows)} unique_urls={len(urls)} failures={len(bad)}\n")
    for b in bad: f.write("\t".join(b) + "\n")
print(f"rows={len(rows)} unique_urls={len(urls)} failures={len(bad)}")
for b in bad[:80]: print(b)
