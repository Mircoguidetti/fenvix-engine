#!/usr/bin/env python3
"""Recompute the coverage numbers from compat/inventory.csv alone.

    python3 compat/coverage.py            # table per category + overall
    python3 compat/coverage.py --detail   # also split by sub-category and list status counts

covered/total counts every row with exclusion_approved != "yes" (an exclusion counts only once the founder approved it; a merely proposed one stays in the denominator). Rows with an approved exclusion are reported separately, by group.
Only engine_status == "covered" counts as covered (partial, untested and missing do not). The script also validates the CSV
(columns, allowed values, covered rows must cite evidence and have differential_test == "yes", unique ids) and exits non-zero on error."""
import collections, csv, os, sys

PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inventory.csv")
COLS = "id category name docs_url driver_reachable exclusion_proposed exclusion_reason exclusion_approved engine_status evidence differential_test".split()
STATUSES = ("covered", "partial", "missing", "untested")


def load(path=PATH):
    with open(path, newline="") as f:
        rd = csv.DictReader(f)
        if rd.fieldnames != COLS:
            sys.exit(f"columns differ: {rd.fieldnames}")
        return list(rd)


def validate(rows):
    errs, seen = [], set()
    for r in rows:
        if r["id"] in seen: errs.append("duplicate id " + r["id"])
        seen.add(r["id"])
        if r["engine_status"] not in STATUSES: errs.append(f"{r['id']}: bad engine_status {r['engine_status']!r}")
        for k in ("driver_reachable", "exclusion_proposed", "exclusion_approved", "differential_test"):
            if r[k] not in ("yes", "no"): errs.append(f"{r['id']}: bad {k} {r[k]!r}")
        if not r["docs_url"].startswith("https://"): errs.append(f"{r['id']}: no docs_url")
        if r["exclusion_proposed"] == "yes" and not r["exclusion_reason"].strip(): errs.append(f"{r['id']}: exclusion without reason")
        if r["exclusion_approved"] == "yes" and r["exclusion_proposed"] != "yes": errs.append(f"{r['id']}: approved exclusion that was not proposed")
        if r["engine_status"] == "covered" and (r["differential_test"] != "yes" or "passing differential tests" not in r["evidence"] and "passing" not in r["evidence"]):
            errs.append(f"{r['id']}: covered without cited passing differential test")
        if r["engine_status"] == "covered" and r["exclusion_proposed"] != "yes" and not r["evidence"].strip():
            errs.append(f"{r['id']}: covered without evidence")
    return errs


def table(rows, keyfn):
    agg = collections.OrderedDict()
    for r in sorted(rows, key=lambda r: keyfn(r)):
        a = agg.setdefault(keyfn(r), collections.Counter())
        a["total"] += 1
        a[r["engine_status"]] += 1
    return agg


def main():
    rows = load()
    errs = validate(rows)
    for e in errs[:50]: print("ERROR", e)
    inscope = [r for r in rows if r["exclusion_approved"] != "yes"]
    excl = [r for r in rows if r["exclusion_approved"] == "yes"]
    detail = "--detail" in sys.argv
    keyfn = (lambda r: r["category"]) if detail else (lambda r: r["category"].split(" / ")[0])
    agg = table(inscope, keyfn)
    w = max(len(k) for k in agg)
    print(f"{'category':{w}}  covered/total  pct   partial untested missing")
    for k, c in agg.items():
        print(f"{k:{w}}  {c['covered']:>5}/{c['total']:<6} {100*c['covered']/c['total']:5.1f}%  {c['partial']:>6} {c['untested']:>8} {c['missing']:>7}")
    tot = len(inscope); cov = sum(r["engine_status"] == "covered" for r in inscope)
    st = collections.Counter(r["engine_status"] for r in inscope)
    print(f"\nAPPROVED EXCLUSIONS APPLIED (in scope): {cov}/{tot} covered = {100*cov/tot:.1f}%   OPEN {tot-cov} (partial {st['partial']}, untested {st['untested']}, missing {st['missing']})")
    allc = sum(r["engine_status"] == "covered" for r in rows)
    print(f"NO EXCLUSION APPLIED (every row counted): {allc}/{len(rows)} covered = {100*allc/len(rows):.1f}%")
    print(f"rows in inventory: {len(rows)}; approved exclusions: {len(excl)} (not in the denominator); proposed but not approved: {sum(r['exclusion_proposed']=='yes' and r['exclusion_approved']!='yes' for r in rows)}")
    return 1 if errs else 0


if __name__ == "__main__":
    sys.exit(main())
