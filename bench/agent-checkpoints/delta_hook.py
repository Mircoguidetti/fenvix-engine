"""Agent checkpoint delta test: per thread, logical bytes (what LangGraph handed to the saver) against stored bytes.

Why: a full-copy checkpointer (LangGraph's default) writes the whole message list at every step, so logical bytes grow
with the square of the thread length while the information grows linearly. The compaction 2 plan (sealed runs per
customer key group) could store consecutive checkpoints of one thread next to each other and compress them against each
other. This test measures how much of that is captured, per thread, on each target:

  logical_bytes     sum over the thread's put + put_writes of the serialized payload (bench.py, per_thread_bytes.json)
  unique_bytes      size of the thread's final message list = what a perfect delta encoding would need (lower bound)
  stored_bytes      what the target stores for this thread (per target below)
  ratio             stored / logical;  delta_capture = (logical - stored) / (logical - unique), 1.0 = perfect delta

Stored bytes per thread, by target:
  postgres  MEASURED: sum of pg_column_size() of the thread's rows in the 3 saver tables (compressed TOAST size; indexes
            and page overhead not included).
  mongo     MEASURED but uncompressed: sum of $bsonSize of the thread's documents (WiredTiger compresses whole blocks,
            it does not report bytes per document). The whole-database compressed figure is in summary.json.
  fenvix    TODO (compaction 2, steps 4-8). The engine has no per-key-range storage report today: there is no
            `fenvixSstInfo` command in mongo-server/src (checked 2026-11-09 on step9-prep). Interface to fill:

              command:  {fenvixSstInfo: "<collection>", prefix: {thread_id: "<id>"}}   on database app_demo
              answer:   {ok: 1, runs: [{kind: "sealed"|"delta"|"l0"|"wal", key: "<object key>",
                                        bytes: <bytes of this range in that object, after compression>,
                                        rows: <rows in range>}],
                         bytes: <sum of runs[].bytes>, logicalBytes: <uncompressed value bytes in range>}

            Steps 4-8 fill it: step 4 (compactor: sealed runs, delta tiers) gives the sealed/delta split; step 6 (counter
            images and slices) can reuse it for slice sizes; step 7 (erasure) can use it to prove a deleted thread has 0
            bytes left in live runs. Until then `fenvix_thread_stored()` returns None and the report says so.

Plugs into bench.py after the sizes phase (before erase): `python delta_hook.py --target ... --uri ... --results DIR`
reads DIR/per_thread_bytes.json and writes DIR/delta_test.json. run_local.sh calls it for every target.
"""
from __future__ import annotations

import argparse
import json
import os

import targets


def postgres_thread_stored(uri: str, thread_id: str) -> int:
    import psycopg

    with psycopg.connect(uri, autocommit=True) as c:
        total = 0
        for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
            total += c.execute(f"SELECT coalesce(sum(pg_column_size(x.*)), 0) FROM {t} x WHERE thread_id = %s",
                               (thread_id,)).fetchone()[0]
        return int(total)


def mongo_thread_stored(uri: str, thread_id: str) -> int:
    from pymongo import MongoClient

    db = MongoClient(uri)[targets.DB_NAME]
    total = 0
    for coll in ("checkpoints", "checkpoint_writes"):
        for r in db[coll].aggregate([{"$match": {"thread_id": thread_id}},
                                     {"$group": {"_id": None, "b": {"$sum": {"$bsonSize": "$$ROOT"}}}}]):
            total += r["b"]
    return total


def fenvix_thread_stored(uri: str, thread_id: str) -> int | None:
    """TODO(compaction 2, steps 4-8): call fenvixSstInfo (interface in the module docstring) and return its `bytes`.
    Returns None until the engine exposes it; the caller reports UNKNOWN."""
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure

    db = MongoClient(uri)[targets.DB_NAME]
    try:
        total = 0
        for coll in ("checkpoints", "checkpoint_writes"):
            total += int(db.command({"fenvixSstInfo": coll, "prefix": {"thread_id": thread_id}})["bytes"])
        return total
    except OperationFailure:  # CommandNotFound today
        return None


STORED = {"postgres": (postgres_thread_stored, "MEASURED pg_column_size (compressed rows, no index/page overhead)"),
          "mongo": (mongo_thread_stored, "MEASURED $bsonSize (UNCOMPRESSED documents; WiredTiger compresses blocks)"),
          "fenvix": (fenvix_thread_stored, "UNKNOWN until fenvixSstInfo exists (compaction 2 steps 4-8)")}


def run(target: str, uri: str, results: str, max_threads: int = 200) -> dict:
    with open(os.path.join(results, "per_thread_bytes.json")) as f:
        per = json.load(f)
    fn, how = STORED[target]
    # the longest threads first: that is where the square growth shows
    ids = sorted(per, key=lambda t: -per[t]["logical_bytes"])[:max_threads]
    rows = []
    for tid in ids:
        p = per[tid]
        stored = fn(uri, tid)
        logical, unique = p["logical_bytes"], p["last_messages_bytes"]
        rows.append({"thread_id": tid, "checkpoints": p["checkpoints"], "logical_bytes": logical, "unique_bytes": unique,
                     "stored_bytes": stored,
                     "ratio": round(stored / logical, 4) if stored is not None and logical else None,
                     "delta_capture": round((logical - stored) / (logical - unique), 4)
                     if stored is not None and logical > unique else None})
    known = [r for r in rows if r["stored_bytes"] is not None]
    out = {"test": "agent checkpoint delta test", "target": target, "stored_bytes_source": how,
           "threads": len(rows),
           "logical_bytes": sum(r["logical_bytes"] for r in rows),
           "unique_bytes": sum(r["unique_bytes"] for r in rows),
           "stored_bytes": sum(r["stored_bytes"] for r in known) if known else None,
           "rows": rows}
    if known:
        lg = sum(r["logical_bytes"] for r in known)
        out["ratio_stored_per_logical"] = round(out["stored_bytes"] / lg, 4)
        out["ratio_unique_per_logical"] = round(out["unique_bytes"] / out["logical_bytes"], 4)
    with open(os.path.join(results, "delta_test.json"), "w") as f:
        json.dump(out, f, indent=1)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", required=True, choices=sorted(STORED))
    ap.add_argument("--uri", required=True)
    ap.add_argument("--results", required=True, help="a bench.py --out directory (needs per_thread_bytes.json)")
    ap.add_argument("--max-threads", type=int, default=200)
    a = ap.parse_args()
    o = run(a.target, a.uri, a.results, a.max_threads)
    print(f"[{a.target}] agent checkpoint delta test: threads={o['threads']} logical={o['logical_bytes']} "
          f"unique={o['unique_bytes']} stored={o['stored_bytes']} ({o['stored_bytes_source']})")


if __name__ == "__main__":
    main()
