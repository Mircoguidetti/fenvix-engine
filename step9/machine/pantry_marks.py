"""Step 9 · the pantry's writing marks, per database (review of the AWS harness, 2026-11-13).

A job that runs a writing program (first-next-ops with saves, reads-after-writes) writes a mark into the kept pantry
(step9-pantry-written/<run>.json) before its extras client starts. A later program that reads a database AS LOADED (the
m30 program, first-next, the ballast rounds: they check answers against, or pick customers from, the state files) must
not run on a database a writing program already changed.

The aws-local-round version blocked on ANY mark in the pantry. In the round's own order (step9/commands/ROUND.txt) job
M2 writes into the largest Atlas-shaped database (first-next-ops saves one message per measured customer), so B1, the
read rounds on the 1 TB database that nothing had written, was refused by `decide.py --pantry-check`, and extras.sh
would have skipped its two programs as PANTRY_WRITTEN. Now a mark names the databases it wrote, and only a reader of one
of those databases is refused.

Database tokens: "atlas" = the five Atlas-shaped databases loaded by the founder's script (the m30 program reads all
five; first-next-ops and reads-after-writes take the largest one); any other token = that database by name (the ballast
database of the command, e.g. app_data_b01 at 1 TB, app_data_h01 at 100 GB). A mark with no readable database list (an
older mark) counts as having written every database.

Usage on the extras client: pantry_marks.py check PANTRY MY_RUN_ID DBS_JSON   (exit 0: may run; 1: written before)"""
import json
import sys

PREFIX = "step9-pantry-written/"
ATLAS = "atlas"
# the aws-local-round marks named the Atlas side with this text
_LEGACY = {"the largest Atlas-shaped database": ATLAS}
ALL = "*"


def written(mark):
    """The set of database tokens a mark says were written; {ALL} when it does not say."""
    dbs = mark.get("databases") if isinstance(mark, dict) else None
    if not isinstance(dbs, list) or not dbs or not all(isinstance(d, str) and d for d in dbs):
        return {ALL}
    return {_LEGACY.get(d, d) for d in dbs}


def blocking(marks, dbs, me=None):
    """The marks (of runs other than `me`) that wrote any of `dbs`."""
    want = set(dbs)
    out = []
    for m in marks:
        if me is not None and str(m.get("run")) == str(me):
            continue
        w = written(m)
        if ALL in w or w & want:
            out.append(m)
    return out


def read_marks(s3, pantry):
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=pantry, Prefix=PREFIX):
        for o in page.get("Contents", []):
            try:
                out.append(json.loads(s3.get_object(Bucket=pantry, Key=o["Key"])["Body"].read()))
            except Exception:  # noqa: BLE001 - an unreadable mark blocks everything (written() of {} is ALL)
                out.append({"run": o["Key"][len(PREFIX):].split(".")[0]})
    return out


def main(argv):
    if len(argv) != 5 or argv[1] != "check":
        sys.exit(__doc__)
    import s3io
    pantry, me, dbs = argv[2], argv[3], json.loads(argv[4] or "[]")
    marks = read_marks(s3io.client(), pantry)
    if not dbs:   # a reader that did not say what it reads: every mark of another run blocks it
        b = [m for m in marks if str(m.get("run")) != str(me)]
    else:
        b = blocking(marks, dbs, me)
    for m in b:
        print(f"written by run {m.get('run')} with {m.get('programs')} into {m.get('databases')}")
    return 1 if b else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
