"""Collect the measured per-thread bytes of every cell into results/competitors.json, next to the value the
earlier sessions published for the same cell.

Measured (this rerun), per thread:
  postgres_default_bytes_per_thread   pg_total_relation_size of checkpoints + checkpoint_blobs +
                                      checkpoint_writes after CHECKPOINT and VACUUM ANALYZE, default mode
  postgres_delta_bytes_per_thread     the same, DeltaChannel mode
  mongo_default_disk_per_thread       storageSize + totalIndexSize of both collections after fsync
  mongo_default_uncompressed_per_thread  size + totalIndexSize of both collections (what Atlas bills)
  (and the same two for Mongo DeltaChannel mode)
Sources: scripted cells run_savers.py disk.json; real cells real_run.py results/real/DS.json; edge cells
edge_cases.py results/edge/N.json for Postgres, and collStats read here for Mongo (edge_cases.py does not
record Mongo bytes).

Published: owner results/summary.json per_thread (scripted cells); reviewer review/results/real/DS.json
per_thread (real cells); reviewer review/results/edge/N.json (edge cells, Postgres only).
"""
import json
import os

from pymongo import MongoClient

import cells as C

HERE = os.path.dirname(os.path.abspath(__file__))
COLLS = ("checkpoints", "checkpoint_writes")


def mongo_stats(db):
    db.client.admin.command("fsync")
    md = {c: {k: db.command("collStats", c)[k] for k in ("count", "size", "storageSize", "totalIndexSize")} for c in COLLS}
    return (sum(v["storageSize"] + v["totalIndexSize"] for v in md.values()),
            sum(v["size"] + v["totalIndexSize"] for v in md.values()))


def pct(a, b):
    return None if a is None or b in (None, 0) else round(100.0 * (a - b) / b, 2)


def main():
    mc = MongoClient(C.MONGO)
    owner = json.load(open(os.path.join(C.SRC_OWNER, "results", "summary.json")))["per_thread"]
    out = []
    for cell in C.cells():
        T = cell["threads"]
        e = {"cell": cell["cell"], "kind": cell["kind"], "threads": T}
        pub = {}
        try:
            if cell["kind"] == "scripted":
                n = cell["n"]
                dd = json.load(open(os.path.join(C.RUN, "results", "savers", f"ck_default_{n}", "disk.json")))
                dl = json.load(open(os.path.join(C.RUN, "results", "savers", f"ck_delta_{n}", "disk.json")))
                assert dd["threads"] == dl["threads"] == T
                e["mean_checkpoints_per_thread"] = n  # run_savers.py exits unless every thread has exactly n
                e["postgres_default_bytes_per_thread"] = dd["postgres"]["total"] / T
                e["postgres_delta_bytes_per_thread"] = dl["postgres"]["total"] / T
                for mode, d in (("default", dd), ("delta", dl)):
                    m = d["mongo"]
                    e[f"mongo_{mode}_disk_per_thread"] = m["total_on_disk"] / T
                    e[f"mongo_{mode}_uncompressed_per_thread"] = sum(m[c]["size"] + m[c]["totalIndexSize"] for c in COLLS) / T
                src = "owner results/summary.json per_thread"
                pub = {"postgres_default_bytes_per_thread": owner[str(n)]["pg_default"],
                       "postgres_delta_bytes_per_thread": owner[str(n)]["pg_delta"],
                       "mongo_default_disk_per_thread": owner[str(n)]["mongo_default_disk"],
                       "mongo_default_uncompressed_per_thread": owner[str(n)]["mongo_default_data"],
                       "mongo_delta_disk_per_thread": owner[str(n)]["mongo_delta_disk"],
                       "mongo_delta_uncompressed_per_thread": owner[str(n)]["mongo_delta_data"]}
            elif cell["kind"] == "real":
                ds = cell["ds"]
                r = json.load(open(os.path.join(C.RUN, "review", "results", "real", ds + ".json")))
                assert r["threads"] == T
                e["mean_checkpoints_per_thread"] = r["checkpoints_per_thread"]["mean"]
                pt = r["per_thread"]
                e["postgres_default_bytes_per_thread"] = pt["pg_default"]
                e["postgres_delta_bytes_per_thread"] = pt["pg_delta"]
                e["mongo_default_disk_per_thread"] = pt["mongo_default_disk"]
                e["mongo_default_uncompressed_per_thread"] = pt["mongo_default_data"]
                e["mongo_delta_disk_per_thread"] = pt["mongo_delta_disk"]
                e["mongo_delta_uncompressed_per_thread"] = pt["mongo_delta_data"]
                src = f"reviewer review/results/real/{ds}.json per_thread"
                p = json.load(open(os.path.join(C.SRC_REV, "results", "real", ds + ".json")))
                pp = p["per_thread"]
                pub = {"mean_checkpoints_per_thread": p["checkpoints_per_thread"]["mean"],
                       "postgres_default_bytes_per_thread": pp["pg_default"],
                       "postgres_delta_bytes_per_thread": pp["pg_delta"],
                       "mongo_default_disk_per_thread": pp["mongo_default_disk"],
                       "mongo_default_uncompressed_per_thread": pp["mongo_default_data"],
                       "mongo_delta_disk_per_thread": pp["mongo_delta_disk"],
                       "mongo_delta_uncompressed_per_thread": pp["mongo_delta_data"]}
                # same threads as the reviewer: per-thread checkpoint counts and logical bytes, in thread order
                e["same_threads_as_reviewer"] = {
                    "checkpoints_equal": r["default"]["checkpoints"] == p["default"]["checkpoints"],
                    # logical = serialized checkpoints of the thread (real_run.py); not byte-identical across runs
                    # (uuid6 ids, timestamps), so compare per thread with a relative difference
                    "logical_bytes_max_rel_diff_pct": round(100 * max(
                        abs(x["logical"] - y["logical"]) / y["logical"]
                        for x, y in zip(r["per_thread_list"], p["per_thread_list"])), 4),
                }
            else:
                n, pol = cell["n"], cell["policy"]
                r = json.load(open(os.path.join(C.RUN, "review", "results", "edge", f"{n}.json")))[pol]
                e["mean_checkpoints_per_thread"] = r["checkpoints_mean"]
                e["postgres_default_bytes_per_thread"] = r["pg_default"]
                e["postgres_delta_bytes_per_thread"] = r["pg_delta"]
                for mode, key in (("default", "mongo_db"), ("delta", "mongo_db_delta")):
                    disk, unc = mongo_stats(mc[cell[key]])
                    e[f"mongo_{mode}_disk_per_thread"] = disk / T
                    e[f"mongo_{mode}_uncompressed_per_thread"] = unc / T
                e["state_mismatches_default_vs_delta"] = r["state_mismatches"]
                src = f"reviewer review/results/edge/{n}.json (Postgres only; Mongo not published)"
                p = json.load(open(os.path.join(C.SRC_REV, "results", "edge", f"{n}.json")))[pol]
                pub = {"mean_checkpoints_per_thread": p["checkpoints_mean"],
                       "postgres_default_bytes_per_thread": p["pg_default"],
                       "postgres_delta_bytes_per_thread": p["pg_delta"]}
        except FileNotFoundError as x:
            e["error"] = f"not measured: {x}"
            out.append(e)
            continue
        e["label"] = "MEASURED (this rerun)"
        e["published"] = {"source": src, **pub}
        e["diff_vs_published_pct"] = {k: pct(e.get(k), v) for k, v in pub.items()}
        dj = os.path.join(C.DUMPS, cell["cell"] + ".json")
        if os.path.exists(dj):
            d = json.load(open(dj))
            e["dump"] = {"file": d["file"], "file_bytes": d["file_bytes"], "docs": d["docs"], "total_bson_bytes": d["total_bson_bytes"]}
        out.append(e)
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    json.dump(out, open(os.path.join(HERE, "results", "competitors.json"), "w"), indent=1)
    for e in out:
        print(e["cell"], {k: round(v) for k, v in e.items() if k.endswith("per_thread") and isinstance(v, (int, float))},
              e.get("diff_vs_published_pct"), e.get("error", ""))


if __name__ == "__main__":
    main()
