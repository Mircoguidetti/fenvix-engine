"""Cases the owner did not run: the message list is rewritten, edited or shrunk.

Policies (on top of the owner's scripted threads, workload.thread_script, seed as the owner's):
  base       the owner's graph, nothing rewritten (control).
  summarize  after a final answer, when the list holds more than 40 messages, a summarize node removes
             every message except the last 4 (RemoveMessage by id) and adds one summary message
             (~1.5 KB). The common "summarisation memory" pattern.
  edit       every tool round also replaces the tool result written 3 rounds earlier with a short
             "[cleared]" stub (same id, add_messages replaces it). The "clear old tool results" pattern.
  remove     every 5th model call removes the 2 oldest messages after the first.

Checks, per policy, on Postgres and Mongo savers, default and DeltaChannel mode:
  A. DeltaChannel state == default state at EVERY checkpoint (messages incl. ids; ToolMessage ids are
     deterministic here), both savers.
  B. Our options rebuild EVERY checkpoint byte for byte: log, JSON chain, binary chain.
  C. Bytes per thread: Postgres default / DeltaChannel on disk against our log.

Usage: edge_cases.py N THREADS  -> results/edge/N.json
"""
import json
import os
import random
import sys
import time

OWNER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, OWNER)

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient
from langgraph.checkpoint.mongodb import MongoDBSaver
from langgraph.checkpoint.postgres import PostgresSaver

import workload as w
import options as o
from run_savers import TeeSaver, PG, MONGO

HERE = os.path.dirname(os.path.abspath(__file__))
POLICIES = ("base", "summarize", "edit", "remove")


def build_graph(mode, policy, checkpointer):
    from langchain_core.messages import AIMessage, RemoveMessage, ToolMessage
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.message import MessagesState

    State = MessagesState if mode == "default" else w.DeltaState
    prose, _ = w.corpus()

    def agent(state, config):
        tid = config["configurable"]["thread_id"]
        out = [next(w.SCRIPTS[tid])]
        msgs = state["messages"]
        if policy == "remove":
            calls = sum(1 for m in msgs if m.type == "ai")
            if calls % 5 == 4 and len(msgs) > 4:
                out = [RemoveMessage(id=m.id) for m in msgs[1:3]] + out
        return {"messages": out}

    def tools(state):
        msgs = state["messages"]
        last = msgs[-1]
        out = [ToolMessage(content=w.RESULTS[c["id"]], name=c["name"], tool_call_id=c["id"], id="tm_" + c["id"])
               for c in last.tool_calls]
        if policy == "edit":
            tms = [m for m in msgs if m.type == "tool" and not str(m.content).startswith("[cleared")]
            ais = [m for m in msgs if m.type == "ai" and m.tool_calls]
            if len(ais) > 3:
                old_ids = {c["id"] for c in ais[-4].tool_calls}
                for m in tms:
                    if m.tool_call_id in old_ids:
                        out.append(ToolMessage(content=f"[cleared {len(str(m.content))} chars]", name=m.name,
                                               tool_call_id=m.tool_call_id, id=m.id))
        return {"messages": out}

    def summarize(state, config):
        msgs = state["messages"]
        rng = random.Random(len(msgs))
        s = w._slice(rng, prose, 1500)
        return {"messages": [RemoveMessage(id=m.id) for m in msgs[:-4]]
                + [AIMessage(content="Summary of the conversation so far: " + s, id=f"sum_{len(msgs)}_{rng.getrandbits(32)}")]}

    def route(state):
        last = state["messages"][-1]
        if getattr(last, "tool_calls", None):
            return "tools"
        if policy == "summarize" and len(state["messages"]) > 40:
            return "summarize"
        return END

    g = StateGraph(State)
    g.add_node("agent", agent)
    g.add_node("tools", tools)
    g.add_node("summarize", summarize)
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", route, {"tools": "tools", "summarize": "summarize", END: END})
    g.add_edge("tools", "agent")
    g.add_edge("summarize", END)
    return g.compile(checkpointer=checkpointer)


def msgs_key(st):
    return [(m.type, m.id, m.content, getattr(m, "tool_call_id", None)) for m in st.values.get("messages", [])]


def main():
    n, threads = int(sys.argv[1]), int(sys.argv[2])
    res = {}
    for policy in POLICIES:
        r = {}
        states = {}
        for mode in ("default", "delta"):
            name = f"edge_{policy}_{mode}_{n}"
            with psycopg.connect(PG + "postgres", autocommit=True) as c:
                c.execute(f"DROP DATABASE IF EXISTS {name}")
                c.execute(f"CREATE DATABASE {name}")
            mc = MongoClient(MONGO)
            mc.drop_database(name)
            pgconn = psycopg.connect(PG + name, autocommit=True, prepare_threshold=0, row_factory=dict_row)
            pg = PostgresSaver(pgconn)
            pg.setup()
            mg = MongoDBSaver(mc, db_name=name)
            g = build_graph(mode, policy, TeeSaver(pg, [mg]))
            for i in range(threads):
                rng = random.Random(f"{w.SEED}-{n}-{i}")
                w.run_thread(g, f"e{n}-{i:04d}", w.thread_script(n, rng))
            # A: state at every checkpoint, through each saver
            for sname, saver in (("postgres", pg), ("mongo", mg)):
                gs = build_graph(mode, policy, saver)
                for i in range(threads):
                    tid = f"e{n}-{i:04d}"
                    ids = [t.config["configurable"]["checkpoint_id"] for t in saver.list({"configurable": {"thread_id": tid}})][::-1]
                    states[(mode, sname, tid)] = [msgs_key(gs.get_state({"configurable": {"thread_id": tid, "checkpoint_id": c}})) for c in ids]
            with pgconn.cursor() as cur:
                cur.execute("CHECKPOINT")
                cur.execute("VACUUM ANALYZE")
                cur.execute("select sum(pg_total_relation_size(c.oid)) s from pg_class c where c.relname in ('checkpoints','checkpoint_blobs','checkpoint_writes')")
                r[f"pg_{mode}"] = int(cur.fetchone()["s"]) / threads
            if mode == "default":
                # B and C on our side
                serde = pg.serde
                tot = {"log": 0, "json": 0, "bin": 0, "logical": 0, "full": 0}
                checked = mism = 0
                cks = []
                for i in range(threads):
                    recs = o.records_for_thread(pg, f"e{n}-{i:04d}")
                    cks.append(len(recs))
                    raws = [serde.dumps_typed(x)[1] for x in recs]
                    tot["logical"] += sum(map(len, raws))
                    tot["full"] += sum(len(o.z(b)) for b in raws)
                    blocks, where, sks = o.log_encode(serde, recs)
                    tot["log"] += sum(map(len, blocks)) + sum(map(len, sks[0]))
                    je = o.json_encode_chain(serde, recs, None)
                    be = o.bin_encode_chain(raws, None)
                    tot["json"] += sum(map(len, je))
                    tot["bin"] += sum(map(len, be))
                    for k in range(len(recs)):
                        ob, _ = o.log_rebuild(serde, blocks, where, sks, k)
                        mism += serde.dumps_typed(ob)[1] != raws[k]
                        if k % 5 == 0 or k == len(recs) - 1:  # chains are O(k) per read: every 5th step + latest
                            ob, _ = o.json_rebuild(serde, je, k, None)
                            mism += serde.dumps_typed(ob)[1] != raws[k]
                            bb, _ = o.bin_rebuild(be, k, None)
                            mism += bb != raws[k]
                            checked += 2
                        checked += 1
                r.update({f"ours_{k}": v / threads for k, v in tot.items()})
                r["rebuild_checked"], r["rebuild_mismatches"] = checked, mism
                r["checkpoints_mean"] = sum(cks) / len(cks)
                r["final_messages_mean"] = sum(len(states[("default", "postgres", f"e{n}-{i:04d}")][-1]) for i in range(threads)) / threads
        comp = mis = 0
        for i in range(threads):
            tid = f"e{n}-{i:04d}"
            ref = states[("default", "postgres", tid)]
            for key in (("default", "mongo", tid), ("delta", "postgres", tid), ("delta", "mongo", tid)):
                other = states[key]
                if len(other) != len(ref):
                    mis += 1
                    comp += 1
                    continue
                for a, b in zip(ref, other):
                    comp += 1
                    mis += a != b
        r["state_compared"], r["state_mismatches"] = comp, mis
        r["pg_default/ours_log"] = r["pg_default"] / r["ours_log"]
        r["pg_delta/ours_log"] = r["pg_delta"] / r["ours_log"]
        res[policy] = r
        print(policy, json.dumps(r), flush=True)
    os.makedirs(os.path.join(HERE, "results", "edge"), exist_ok=True)
    json.dump(res, open(os.path.join(HERE, "results", "edge", f"{n}.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
