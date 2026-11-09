"""Read cost on the savers as they are: graph.get_state for the latest step and an old step
(k = N/4), default mode against DeltaChannel mode, PostgresSaver and MongoDBSaver, local
servers, warm caches (median of 3 after one unmeasured call). Also checks that the delta-mode
state equals the default-mode state for every measured read.

Usage: saver_reads.py N [threads]  ->  results/saver-reads/N.json
"""
import json
import os
import statistics
import sys
import time

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient
from langgraph.checkpoint.mongodb import MongoDBSaver
from langgraph.checkpoint.postgres import PostgresSaver

import workload as w

PG = "postgresql://postgres:pg@127.0.0.1:5432/"
HERE = os.path.dirname(os.path.abspath(__file__))


def savers(mode, n):
    name = f"ck_{mode}_{n}"
    pg = PostgresSaver(psycopg.connect(PG + name, autocommit=True, prepare_threshold=0, row_factory=dict_row))
    mg = MongoDBSaver(MongoClient("mongodb://127.0.0.1:27017/"), db_name=name)
    return {"postgres": pg, "mongo": mg}


def main():
    n = int(sys.argv[1])
    threads = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    res, mism, checked = {}, 0, 0
    ref = {}
    for mode in ("default", "delta"):
        for sname, saver in savers(mode, n).items():
            g = w.build_graph(mode, saver)
            for i in range(threads):
                tid = f"t{n}-{i:04d}"
                ids = [t.config["configurable"]["checkpoint_id"] for t in saver.list({"configurable": {"thread_id": tid}})][::-1]
                for label, k in (("latest", None), ("old", n // 4)):
                    cfg = {"configurable": {"thread_id": tid}}
                    if k is not None:
                        cfg["configurable"]["checkpoint_id"] = ids[k]
                    g.get_state(cfg)
                    ts = []
                    for _ in range(3):
                        t = time.perf_counter()
                        st = g.get_state(cfg)
                        ts.append(time.perf_counter() - t)
                    # ToolNode gives ToolMessages random ids, which differ between the two runs: compare the rest
                    msgs = [(m.type, m.content, getattr(m, "tool_call_id", None), m.id if m.type != "tool" else None) for m in st.values.get("messages", [])]
                    key = (tid, label)
                    if key in ref:
                        checked += 1
                        mism += ref[key] != msgs
                    else:
                        ref[key] = msgs
                    res.setdefault(f"{mode}/{sname}/{label}", []).append(statistics.median(ts) * 1000)
    out = {
        "n": n,
        "threads": threads,
        "ms_median": {k: statistics.median(v) for k, v in res.items()},
        "ms_max": {k: max(v) for k, v in res.items()},
        "state_compared": checked,
        "state_mismatches": mism,
    }
    os.makedirs(os.path.join(HERE, "results", "saver-reads"), exist_ok=True)
    json.dump(out, open(os.path.join(HERE, "results", "saver-reads", f"{n}.json"), "w"), indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
