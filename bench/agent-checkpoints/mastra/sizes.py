"""Bytes Mastra's stores keep for the benchmark database "app_chat": usage  python mastra/sizes.py mongo|pg URI
Mongo: dbStats (dataSize = logical, storageSize + indexSize = stored on disk; on our server storageSize = dataSize and
the real stored bytes are the S3 objects under tenants/app_chat/). Postgres: pg_total_relation_size of every mastra_* table."""
import json
import sys

kind, uri = sys.argv[1], sys.argv[2]
if kind == "mongo":
    from pymongo import MongoClient

    c = MongoClient(uri)
    try:
        c.admin.command("fsync")
    except Exception:  # noqa: BLE001 - our server or Atlas may refuse it
        pass
    s = c["app_chat"].command("dbStats")
    print(json.dumps({k: s.get(k) for k in ("objects", "dataSize", "storageSize", "indexSize")}))
else:
    import psycopg

    with psycopg.connect(uri) as conn:
        rows = conn.execute("SELECT relname, pg_total_relation_size(oid) FROM pg_class WHERE relkind = 'r' AND relname LIKE 'mastra%'").fetchall()
        tables = {r[0]: r[1] for r in rows if r[1] > 0}
        print(json.dumps({"stored_bytes": sum(r[1] for r in rows), "tables_nonempty": tables}))
