"""Targets of the agent checkpoint benchmark: how each database is reached through the REAL framework package, and how
each one reports the bytes it stores.

  fenvix   -> langgraph.checkpoint.mongodb.MongoDBSaver pointed at our server (mongo-server); stored bytes = objects in
              the S3 bucket under tenants/<db>/ (the only copy of the data), logical bytes from dbStats.
  mongo    -> the same MongoDBSaver on mongod / Atlas; stored bytes = dbStats storageSize + indexSize (WiredTiger, compressed).
  postgres -> langgraph.checkpoint.postgres.PostgresSaver on Postgres / Aurora / Neon / Supabase; stored bytes =
              pg_total_relation_size of the saver's tables (heap + TOAST + indexes).

Every target uses the database name "app_demo" by default (neutral name, see runbooks/).
Nothing here talks to a cloud service by itself: the URIs come from the command line (locally) or from GitHub secrets
(AWS round, see runbooks/).
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Callable

DB_NAME = "app_demo"


# ----------------------------------------------------------------------------------------------------------------------
# Saver construction
# ----------------------------------------------------------------------------------------------------------------------


def mongodb_saver(uri: str, db_name: str = DB_NAME):
    """The official LangGraph MongoDB checkpointer (langgraph-checkpoint-mongodb), unchanged."""
    from langgraph.checkpoint.mongodb import MongoDBSaver
    from pymongo import MongoClient

    client = MongoClient(uri)
    return MongoDBSaver(client, db_name=db_name), client


def fenvix_saver(uri: str, db_name: str = DB_NAME):
    """The checkpointer an app uses with Fenvix today: the official MongoDB saver, pointed at our server.

    This is the single place a future thin `fenvix` package would wrap (e.g. `from fenvix.langgraph import FenvixSaver`,
    which would only fix defaults such as the URI scheme, the database per customer and, later, any engine hint for
    checkpoint deltas). Keep the benchmark calling this function so the package can replace it one-for-one."""
    return mongodb_saver(uri, db_name)


def postgres_saver(uri: str):
    """The official LangGraph Postgres checkpointer (langgraph-checkpoint-postgres), with the connection settings its
    documentation asks for (autocommit, dict rows, prepare_threshold=0 for poolers such as Supabase/Neon pgbouncer)."""
    import psycopg
    from langgraph.checkpoint.postgres import PostgresSaver
    from psycopg.rows import dict_row

    conn = psycopg.connect(uri, autocommit=True, prepare_threshold=0, row_factory=dict_row)
    saver = PostgresSaver(conn)
    saver.setup()
    return saver, conn


# ----------------------------------------------------------------------------------------------------------------------
# Target: a saver factory plus the size and reset hooks
# ----------------------------------------------------------------------------------------------------------------------


@dataclass
class Target:
    name: str  # fenvix | mongo | postgres
    uri: str
    label: str = ""  # free text, e.g. "local mongod 7.0" or "Aurora I/O-Optimized db.r7g.large"
    s3: dict = field(default_factory=dict)  # fenvix only: endpoint, bucket, access keys (MinIO locally)

    def make_saver(self):
        """Return (saver, handle_to_close). One per worker."""
        if self.name == "fenvix":
            return fenvix_saver(self.uri)
        if self.name == "mongo":
            return mongodb_saver(self.uri)
        if self.name == "postgres":
            return postgres_saver(self.uri)
        raise ValueError(self.name)

    # -- reset: empty database before a run -------------------------------------------------------------------------
    def reset(self) -> None:
        if self.name in ("fenvix", "mongo"):
            from pymongo import MongoClient

            MongoClient(self.uri)[DB_NAME].command("dropDatabase")
        else:
            import psycopg

            with psycopg.connect(self.uri, autocommit=True) as c:
                for t in ("checkpoint_writes", "checkpoint_blobs", "checkpoints", "checkpoint_migrations"):
                    c.execute(f"DROP TABLE IF EXISTS {t} CASCADE")

    # -- sizes ------------------------------------------------------------------------------------------------------
    def sizes(self) -> dict:
        """What the target reports it stores. `stored_bytes` is the number used for bytes stored per logical byte."""
        if self.name == "postgres":
            return _pg_sizes(self.uri)
        out = _mongo_sizes(self.uri)
        if self.name == "mongo":
            out["stored_bytes"] = out["dbStats"]["storageSize"] + out["dbStats"]["indexSize"]
            out["stored_from"] = "dbStats storageSize + indexSize"
        else:
            s3b = s3_prefix_bytes(self.s3, f"tenants/{DB_NAME}/") if self.s3 else None
            out["s3"] = s3b
            out["stored_bytes"] = s3b["total"] if s3b else None
            out["stored_from"] = f"S3 objects under tenants/{DB_NAME}/" if s3b else "UNKNOWN (no S3 access given)"
            out["serverStatus_s3"] = fenvix_s3_counters(self.uri)
        return out


def _mongo_sizes(uri: str) -> dict:
    from pymongo import MongoClient

    client = MongoClient(uri)
    try:
        # WiredTiger updates storageSize only at its checkpoints (every 60 s); fsync forces one. Atlas may refuse it.
        client.admin.command("fsync")
    except Exception:  # noqa: BLE001
        pass
    db = client[DB_NAME]
    s = db.command("dbStats")
    colls = {}
    for c in db.list_collection_names():
        cs = db.command("collStats", c)
        colls[c] = {k: cs.get(k) for k in ("count", "size", "storageSize", "totalIndexSize")}
    return {"dbStats": {k: s.get(k) for k in ("objects", "dataSize", "storageSize", "indexSize", "totalSize")},
            "collStats": colls}


def _pg_sizes(uri: str) -> dict:
    import psycopg

    with psycopg.connect(uri, autocommit=True) as c:
        tables = {}
        for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes", "checkpoint_migrations"):
            row = c.execute("SELECT pg_total_relation_size(%s), pg_relation_size(%s), (SELECT count(*) FROM " + t + ")",
                            (t, t)).fetchone()
            tables[t] = {"total": row[0], "heap": row[1], "rows": row[2]}
        dbsize = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    return {"tables": tables, "pg_database_size": dbsize,
            "stored_bytes": sum(v["total"] for v in tables.values()),
            "stored_from": "sum of pg_total_relation_size of the 4 saver tables (heap + TOAST + indexes)"}


def s3_prefix_bytes(s3: dict, prefix: str) -> dict:
    """Bytes and objects under a prefix, grouped by the next path element (e.g. wal, compacted, manifest)."""
    import boto3

    cli = boto3.client("s3", endpoint_url=s3.get("endpoint") or None, region_name=s3.get("region", "us-east-1"),
                       aws_access_key_id=s3.get("key"), aws_secret_access_key=s3.get("secret"))
    by, cnt = {}, {}
    for page in cli.get_paginator("list_objects_v2").paginate(Bucket=s3["bucket"], Prefix=prefix):
        for o in page.get("Contents", []):
            part = o["Key"][len(prefix):].split("/")[0]
            by[part] = by.get(part, 0) + o["Size"]
            cnt[part] = cnt.get(part, 0) + 1
    return {"total": sum(by.values()), "objects": sum(cnt.values()), "by_kind": by, "count_by_kind": cnt}


def fenvix_s3_counters(uri: str) -> dict | None:
    """S3 request counters our server exposes in serverStatus.fenvix.s3 (PUT, GET, ...), process-wide."""
    from pymongo import MongoClient

    try:
        return dict(MongoClient(uri).admin.command("serverStatus").get("fenvix", {}).get("s3", {}))
    except Exception as e:  # noqa: BLE001 - informative only
        return {"error": str(e)}


def wait_settled(t: Target, seconds: float = 5.0) -> None:
    """Our server uploads to S3 in the background; give it a moment before listing the bucket."""
    if t.name == "fenvix":
        time.sleep(seconds)


def from_args(name: str, uri: str, label: str = "") -> Target:
    s3 = {}
    if name == "fenvix" and os.environ.get("BENCH_S3_BUCKET"):
        s3 = {"endpoint": os.environ.get("BENCH_S3_ENDPOINT", ""), "bucket": os.environ["BENCH_S3_BUCKET"],
              "region": os.environ.get("BENCH_S3_REGION", "eu-west-1"),
              "key": os.environ.get("BENCH_S3_KEY") or None, "secret": os.environ.get("BENCH_S3_SECRET") or None}
    return Target(name, uri, label, s3)


SAVER_FACTORIES: dict[str, Callable] = {"fenvix": fenvix_saver, "mongo": mongodb_saver, "postgres": postgres_saver}
