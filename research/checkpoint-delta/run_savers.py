"""Run the simulated agents through the real savers and capture what they store.

For each mode (default, delta) and each thread length N, one LangGraph graph runs every
thread; a tee checkpointer sends every put / put_writes to three real savers at once:
PostgresSaver (primary: reads and versions come from it), MongoDBSaver and SqliteSaver.
Each (mode, N) gets its own Postgres database, Mongo database and SQLite file, so the
bytes on disk belong to that cell alone.

Then, per cell:
  rows.csv.gz  one line per row / document each saver wrote: table, thread, checkpoint, raw bytes, stored bytes
  disk.json    on-disk bytes: Postgres heap + TOAST + indexes per table, Mongo storageSize + indexes
               (WiredTiger, snappy) and dataSize, SQLite file size.

Usage: run_savers.py MODE N THREADS [--pg-toast lz4]
"""
import argparse
import csv
import gzip
import json
import os
import random
import sqlite3
import sys
import time

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.mongodb import MongoDBSaver
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.checkpoint.sqlite import SqliteSaver

import workload as w

PG = "postgresql://postgres:pg@127.0.0.1:5432/"
MONGO = "mongodb://127.0.0.1:27017/"
HERE = os.path.dirname(os.path.abspath(__file__))


class TeeSaver(BaseCheckpointSaver):
    """Writes go to every saver; reads and version numbers come from the primary."""

    def __init__(self, primary, others):
        super().__init__(serde=primary.serde)
        self.primary, self.all = primary, [primary, *others]

    def get_tuple(self, config):
        return self.primary.get_tuple(config)

    def list(self, config, **kw):
        return self.primary.list(config, **kw)

    def put(self, config, checkpoint, metadata, new_versions):
        out = None
        for s in self.all:
            r = s.put(config, checkpoint, metadata, new_versions)
            out = out or r
        return out

    def put_writes(self, config, writes, task_id, task_path=""):
        for s in self.all:
            s.put_writes(config, writes, task_id, task_path)

    def get_next_version(self, current, channel):
        return self.primary.get_next_version(current, channel)

    def get_delta_channel_history(self, *, config, channels):
        return self.primary.get_delta_channel_history(config=config, channels=channels)


def cell_name(mode, n, toast):
    return f"ck_{mode}_{n}" + ("_lz4" if toast == "lz4" else "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["default", "delta"])
    ap.add_argument("n", type=int)
    ap.add_argument("threads", type=int)
    ap.add_argument("--pg-toast", default="pglz", choices=["pglz", "lz4"])
    ap.add_argument("--pg-only", action="store_true", help="only the Postgres saver (TOAST variant runs)")
    a = ap.parse_args()
    name = cell_name(a.mode, a.n, a.pg_toast)
    out = os.path.join(HERE, "results", "savers", name)
    os.makedirs(out, exist_ok=True)

    # fresh stores
    with psycopg.connect(PG + "postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {name}")
        c.execute(f"CREATE DATABASE {name}")
        if a.pg_toast == "lz4":
            c.execute(f"ALTER DATABASE {name} SET default_toast_compression = 'lz4'")
    mc = MongoClient(MONGO)
    mc.drop_database(name)
    sqlite_path = os.path.join(out, "sqlite.db")
    for suf in ("", "-wal", "-shm"):
        if os.path.exists(sqlite_path + suf):
            os.remove(sqlite_path + suf)

    pgconn = psycopg.connect(PG + name, autocommit=True, prepare_threshold=0, row_factory=dict_row)
    pg = PostgresSaver(pgconn)
    pg.setup()
    others = []
    if not a.pg_only:
        mg = MongoDBSaver(mc, db_name=name)
        sq = SqliteSaver(sqlite3.connect(sqlite_path, check_same_thread=False))
        sq.setup()
        others = [mg, sq]
    graph = w.build_graph(a.mode, TeeSaver(pg, others))

    t0 = time.time()
    for i in range(a.threads):
        rng = random.Random(f"{w.SEED}-{a.n}-{i}")
        script = w.thread_script(a.n, rng)
        tid = f"t{a.n}-{i:04d}"
        w.run_thread(graph, tid, script)
        got = len(list(pg.list({"configurable": {"thread_id": tid}})))
        if got != a.n:
            sys.exit(f"{tid}: {got} checkpoints, expected {a.n}")
    run_s = time.time() - t0

    # ---- per row capture
    rows = []
    with pgconn.cursor() as cur:
        cur.execute(
            "select thread_id, checkpoint_id, octet_length(checkpoint::text) + octet_length(metadata::text) raw,"
            " pg_column_size(checkpoint) + pg_column_size(metadata) stored from checkpoints"
        )
        rows += [("pg.checkpoints", r["thread_id"], r["checkpoint_id"], r["raw"], r["stored"]) for r in cur]
        cur.execute(
            "select thread_id, channel || ':' || version id, coalesce(octet_length(blob),0) raw,"
            " coalesce(pg_column_size(blob),0) stored from checkpoint_blobs"
        )
        rows += [("pg.checkpoint_blobs", r["thread_id"], r["id"], r["raw"], r["stored"]) for r in cur]
        cur.execute(
            "select thread_id, checkpoint_id || ':' || task_id || ':' || idx id, octet_length(blob) raw,"
            " pg_column_size(blob) stored from checkpoint_writes"
        )
        rows += [("pg.checkpoint_writes", r["thread_id"], r["id"], r["raw"], r["stored"]) for r in cur]
    if not a.pg_only:
        db = mc[name]
        for coll in ("checkpoints", "checkpoint_writes"):
            for d in db[coll].aggregate(
                [{"$project": {"thread_id": 1, "checkpoint_id": 1, "task_id": 1, "idx": 1, "s": {"$bsonSize": "$$ROOT"}}}]
            ):
                rows.append((f"mongo.{coll}", d["thread_id"], f'{d["checkpoint_id"]}:{d.get("task_id","")}:{d.get("idx","")}', d["s"], ""))
        sc = sqlite3.connect(sqlite_path)
        for tid, cid, raw in sc.execute("select thread_id, checkpoint_id, length(checkpoint)+length(metadata) from checkpoints"):
            rows.append(("sqlite.checkpoints", tid, cid, raw, ""))
        for tid, cid, raw in sc.execute("select thread_id, checkpoint_id||':'||task_id||':'||idx, length(value) from writes"):
            rows.append(("sqlite.writes", tid, cid, raw, ""))
        sc.close()
    with gzip.open(os.path.join(out, "rows.csv.gz"), "wt", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["table", "thread_id", "row_id", "raw_bytes", "stored_bytes"])
        wr.writerows(rows)

    # ---- on disk
    disk = {"mode": a.mode, "n": a.n, "threads": a.threads, "pg_toast": a.pg_toast, "run_seconds": round(run_s, 1)}
    with pgconn.cursor() as cur:
        cur.execute("CHECKPOINT")
        cur.execute("VACUUM ANALYZE")
        pgd = {}
        for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
            cur.execute(
                "select pg_relation_size(c.oid) heap,"
                " coalesce(pg_relation_size(c.reltoastrelid),0) toast,"
                " coalesce(pg_indexes_size(c.reltoastrelid),0) toast_index,"
                " pg_indexes_size(c.oid) indexes, pg_total_relation_size(c.oid) total"
                " from pg_class c where c.relname = %s",
                (t,),
            )
            pgd[t] = cur.fetchone()
        pgd["total"] = sum(v["total"] for v in pgd.values())
        disk["postgres"] = pgd
    if not a.pg_only:
        mc.admin.command("fsync")
        md = {}
        for coll in ("checkpoints", "checkpoint_writes"):
            s = db.command("collStats", coll)
            md[coll] = {k: s[k] for k in ("count", "size", "storageSize", "totalIndexSize")}
        md["total_on_disk"] = sum(v["storageSize"] + v["totalIndexSize"] for v in md.values())
        md["total_data_size"] = sum(v["size"] for k, v in md.items() if isinstance(v, dict))
        disk["mongo"] = md
        sc = sqlite3.connect(sqlite_path)
        sc.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        sc.close()
        disk["sqlite"] = {"file_bytes": os.path.getsize(sqlite_path)}
    json.dump(disk, open(os.path.join(out, "disk.json"), "w"), indent=1)
    if not a.pg_only:
        os.remove(sqlite_path)  # the size is recorded; the file itself is not kept
    print(json.dumps(disk))


if __name__ == "__main__":
    main()
