#!/usr/bin/env python3
"""Apply the per-item evidence of the compat-gaps work to inventory.csv and re-validate with coverage.py.

    python3 compat/apply_gaps.py compat/gaps_status/item01.csv [...]

Each status file has the columns  id,engine_status,evidence,differential_test  (the evidence names the passing gap tests and the
driver counts).  Only the rows named are changed; coverage.py's validation (a covered row must cite a passing differential test)
runs afterwards and its table is printed."""
import csv, subprocess, sys, os

HERE = os.path.dirname(os.path.abspath(__file__))
INV = os.path.join(HERE, "inventory.csv")


def main():
    with open(INV, newline="") as f:
        rd = csv.DictReader(f)
        cols = rd.fieldnames
        rows = list(rd)
    byid = {r["id"]: r for r in rows}
    n = 0
    for path in sys.argv[1:]:
        for u in csv.DictReader(open(path, newline="")):
            r = byid.get(u["id"])
            if r is None:
                sys.exit(f"{path}: unknown id {u['id']}")
            r["engine_status"], r["evidence"], r["differential_test"] = u["engine_status"], u["evidence"], u["differential_test"]
            n += 1
    with open(INV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"{n} rows updated")
    subprocess.run([sys.executable, os.path.join(HERE, "coverage.py")], check=False)


if __name__ == "__main__":
    main()
