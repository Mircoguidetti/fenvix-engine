"""Run real agent trajectories (convert_real.py scripts) through the real LangGraph savers and through
our options, with the owner's code (research/checkpoint-delta) wherever possible.

Per dataset:
  1. savers: default and delta mode, TeeSaver -> PostgresSaver + MongoDBSaver (+ SQLite), own databases
     rv_<ds>_<mode>; on-disk bytes measured exactly like run_savers.py (CHECKPOINT + VACUUM ANALYZE, fsync).
  2. ours, on the default-mode Postgres records (options.records_for_thread): full zstd, binary chain,
     JSON chain, thread message log (zstd and lz4), and the envelope of the Mongo saver documents
     (every field except the checkpoint/metadata/value payloads, BSON, zstd per thread) = keys overhead.
  3. rebuild check: log rebuild of EVERY checkpoint of the first CHECK threads, byte for byte.

Usage: real_run.py DS [max_threads] [--sample N]  -> results/real/DS.json
"""
import argparse
import json
import os
import random
import sqlite3
import statistics
import sys
import time

OWNER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, OWNER)

import bson
import lz4.frame
import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient
from langgraph.checkpoint.mongodb import MongoDBSaver
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.checkpoint.sqlite import SqliteSaver

import workload as w
import options as o
from run_savers import TeeSaver, PG, MONGO

HERE = os.path.dirname(os.path.abspath(__file__))
CHECK = 3


def build_graph(mode, checkpointer):
    """The owner's graph (scripted model, tools_condition), with a tool node that answers any tool
    name with the recorded result. Same ToolMessage fields as the prebuilt ToolNode."""
    from langchain_core.messages import ToolMessage
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.message import MessagesState
    from langgraph.prebuilt import tools_condition

    State = MessagesState if mode == "default" else w.DeltaState

    def agent(state, config):
        return {"messages": [next(w.SCRIPTS[config["configurable"]["thread_id"]])]}

    def tools(state):
        last = state["messages"][-1]
        return {"messages": [ToolMessage(content=w.RESULTS[c["id"]], name=c["name"], tool_call_id=c["id"]) for c in last.tool_calls]}

    g = StateGraph(State)
    g.add_node("agent", agent)
    g.add_node("tools", tools)
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", tools_condition, {"tools": "tools", END: END})
    g.add_edge("tools", "agent")
    return g.compile(checkpointer=checkpointer)


def run_savers(ds, mode, scripts, with_sqlite=True):
    name = f"rv_{ds}_{mode}".lower()
    with psycopg.connect(PG + "postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {name}")
        c.execute(f"CREATE DATABASE {name}")
    mc = MongoClient(MONGO)
    mc.drop_database(name)
    sqlite_path = os.path.join(HERE, f"{name}.db")
    for suf in ("", "-wal", "-shm"):
        if os.path.exists(sqlite_path + suf):
            os.remove(sqlite_path + suf)
    pgconn = psycopg.connect(PG + name, autocommit=True, prepare_threshold=0, row_factory=dict_row)
    pg = PostgresSaver(pgconn)
    pg.setup()
    mg = MongoDBSaver(mc, db_name=name)
    others = [mg]
    if with_sqlite:
        sq = SqliteSaver(sqlite3.connect(sqlite_path, check_same_thread=False))
        sq.setup()
        others.append(sq)
    graph = build_graph(mode, TeeSaver(pg, others))
    lens = []
    for i, script in enumerate(scripts):
        tid = f"{ds}-{i:04d}"
        w.run_thread(graph, tid, script)
        got = len(list(pg.list({"configurable": {"thread_id": tid}})))
        exp = sum(3 + 2 * len(t["rounds"]) for t in script)
        if got != exp:
            sys.exit(f"{tid}: {got} checkpoints, expected {exp}")
        lens.append(got)
    disk = {"threads": len(scripts), "checkpoints": lens}
    with pgconn.cursor() as cur:
        cur.execute("CHECKPOINT")
        cur.execute("VACUUM ANALYZE")
        pgd = {}
        for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"):
            cur.execute("select pg_total_relation_size(c.oid) total from pg_class c where c.relname = %s", (t,))
            pgd[t] = cur.fetchone()["total"]
        pgd["total"] = sum(pgd.values())
        disk["postgres"] = pgd
    mc.admin.command("fsync")
    db = mc[name]
    md = {}
    for coll in ("checkpoints", "checkpoint_writes"):
        s = db.command("collStats", coll)
        md[coll] = {k: s[k] for k in ("count", "size", "storageSize", "totalIndexSize")}
    disk["mongo_disk"] = sum(v["storageSize"] + v["totalIndexSize"] for v in md.values())
    disk["mongo_data_plus_index"] = sum(v["size"] + v["totalIndexSize"] for v in md.values())
    disk["mongo"] = md
    if with_sqlite:
        sc = sqlite3.connect(sqlite_path)
        sc.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        sc.close()
        disk["sqlite"] = os.path.getsize(sqlite_path)
        os.remove(sqlite_path)
    return name, disk


def envelope_bytes(db, tid):
    """Keys and document envelope the Mongo saver stores beside the payloads, compressed per thread."""
    raw = b""
    for coll, drop in (("checkpoints", ("checkpoint", "metadata")), ("checkpoint_writes", ("value",))):
        for d in db[coll].find({"thread_id": tid}).sort([("checkpoint_id", 1)]):
            for k in drop:
                d.pop(k, None)
            raw += bson.encode(d)
    return len(o.z(raw)), len(raw)


def ours(pgname, mgname, nthreads):
    conn = psycopg.connect(PG + pgname, autocommit=True, prepare_threshold=0, row_factory=dict_row)
    saver = PostgresSaver(conn)
    serde = saver.serde
    db = MongoClient(MONGO)[mgname]
    tids = [r["thread_id"] for r in conn.execute("select distinct thread_id from checkpoints order by 1")][:nthreads]
    tot = {k: 0 for k in ("logical", "full", "bin", "json", "log", "log_lz4", "envelope", "envelope_raw")}
    per_thread = []
    checked = mism = 0
    reads = {"latest": {"full": [], "log": []}, "old": {"full": [], "log": []}}
    for ti, tid in enumerate(tids):
        recs = o.records_for_thread(saver, tid)
        raws = [serde.dumps_typed(r)[1] for r in recs]
        full = [o.z(b) for b in raws]
        t = {"n": len(recs), "logical": sum(map(len, raws)), "full": sum(map(len, full))}
        t["bin"] = sum(map(len, o.bin_encode_chain(raws, None)))
        t["json"] = sum(map(len, o.json_encode_chain(serde, recs, None)))
        blocks, where, sks = o.log_encode(serde, recs)
        t["log"] = sum(map(len, blocks)) + sum(map(len, sks[0]))
        zs = o.z
        o.z = lambda b: lz4.frame.compress(b)
        try:
            b2, _, s2 = o.log_encode(serde, recs)
        finally:
            o.z = zs
        t["log_lz4"] = sum(map(len, b2)) + sum(map(len, s2[0]))
        t["envelope"], t["envelope_raw"] = envelope_bytes(db, tid)
        for k in tot:
            tot[k] += t[k]
        per_thread.append(t)
        if ti < CHECK:
            for k in range(len(recs)):
                ob, _ = o.log_rebuild(serde, blocks, where, sks, k)
                checked += 1
                mism += serde.dumps_typed(ob)[1] != raws[k]
        if ti < 4:
            n = len(recs)
            for label, k in (("latest", n - 1), ("old", n // 4)):
                _, tf = o.timed(lambda: serde.loads_typed(("msgpack", o.unz(full[k]))))
                (ob, nb), tl = o.timed(lambda: o.log_rebuild(serde, blocks, where, sks, k))
                reads[label]["full"].append((len(full[k]), tf * 1000))
                reads[label]["log"].append((nb, tl * 1000))
    rd = {lab: {k: {"bytes_mean": statistics.mean(x[0] for x in v), "ms_median": statistics.median(x[1] for x in v)}
                for k, v in d.items()} for lab, d in reads.items()}
    return tot, per_thread, checked, mism, rd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ds")
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--no-sqlite", action="store_true")
    a = ap.parse_args()
    scripts = [json.loads(l) for l in open(os.path.join(HERE, "real", a.ds + ".jsonl"))]
    if a.sample and a.sample < len(scripts):
        scripts = random.Random(20261109).sample(scripts, a.sample)
    out = {"ds": a.ds, "threads": len(scripts)}
    t0 = time.time()
    for mode in ("default", "delta"):
        name, disk = run_savers(a.ds, mode, scripts, not a.no_sqlite)
        out[mode] = disk
        print(a.ds, mode, f"{time.time()-t0:.0f}s", json.dumps({k: v for k, v in disk.items() if k != "checkpoints"}), flush=True)
    tot, per, checked, mism, rd = ours(f"rv_{a.ds}_default".lower(), f"rv_{a.ds}_default".lower(), len(scripts))
    T = len(scripts)
    pt = {"pg_default": out["default"]["postgres"]["total"] / T, "pg_delta": out["delta"]["postgres"]["total"] / T,
          "mongo_default_disk": out["default"]["mongo_disk"] / T, "mongo_delta_disk": out["delta"]["mongo_disk"] / T,
          "mongo_default_data": out["default"]["mongo_data_plus_index"] / T, "mongo_delta_data": out["delta"]["mongo_data_plus_index"] / T}
    pt.update({"ours_" + k: v / T for k, v in tot.items()})
    out["per_thread"] = pt
    out["ratios"] = {
        "pg_default/ours_log": pt["pg_default"] / pt["ours_log"],
        "pg_delta/ours_log": pt["pg_delta"] / pt["ours_log"],
        "pg_default/ours_log+env": pt["pg_default"] / (pt["ours_log"] + pt["ours_envelope"]),
        "pg_delta/ours_log+env": pt["pg_delta"] / (pt["ours_log"] + pt["ours_envelope"]),
        "pg_default/ours_log_lz4": pt["pg_default"] / pt["ours_log_lz4"],
        "pg_delta/ours_log_lz4": pt["pg_delta"] / pt["ours_log_lz4"],
        "mongo_default_disk/ours_log": pt["mongo_default_disk"] / pt["ours_log"],
        "mongo_delta_disk/ours_log": pt["mongo_delta_disk"] / pt["ours_log"],
        "pg_default/pg_delta": pt["pg_default"] / pt["pg_delta"],
        "pg_default/ours_full": pt["pg_default"] / pt["ours_full"],
        "envelope/log": pt["ours_envelope"] / pt["ours_log"],
    }
    ck = sorted(out["default"]["checkpoints"])
    out["checkpoints_per_thread"] = {"min": ck[0], "median": ck[len(ck) // 2], "mean": sum(ck) / len(ck), "max": ck[-1]}
    out["per_thread_list"] = per
    out["rebuild_checked"], out["rebuild_mismatches"] = checked, mism
    out["reads"] = rd
    os.makedirs(os.path.join(HERE, "results", "real"), exist_ok=True)
    json.dump(out, open(os.path.join(HERE, "results", "real", a.ds + ".json"), "w"), indent=1)
    print(json.dumps({k: out[k] for k in ("ds", "threads", "checkpoints_per_thread", "per_thread", "ratios", "rebuild_checked", "rebuild_mismatches", "reads")}, indent=1))


if __name__ == "__main__":
    main()
