"""Server check of derived documents (fenvix-brain prompts/P-c2-delta-log.md, tasks 4 and 6).

The real LangGraph MongoDB saver (langgraph-checkpoint-mongodb 0.5.1, the checkpoint-delta harness's pinned venv) runs
the owner's scripted agents against mongo-server with sealed runs, the compactor, derived documents on (release flag
E1, `--sealed-derive`), zstd sealed runs and the saver's two collections customer-keyed by `thread_id`. Then:

1. snapshot A: every stored document of both collections as raw BSON, and per thread the saver's get_tuple of the
   latest and of an old checkpoint, list() of the whole history, and graph.get_state;
2. `fenvixReseal`: every group resealed, the threads become message rows plus derived documents (serverStatus
   `fenvix.derived.encoded` > 0); snapshot B must equal A byte for byte;
3. more steps on every thread (young rows on top of derived ones), snapshot C; reseal again (derived documents rebuilt
   and re-encoded), snapshot D must equal C;
4. the saver deletes some threads (delete_thread), reseal: their documents are gone, the others unchanged;
5. the server restarts on the same bucket (cold reads of the derived runs): snapshot equal again.

Usage: server_check.py OWNER_HARNESS_DIR (the owner's research/checkpoint-delta with workload.py). Needs MinIO on 9000
(judge/setup.sh) and target/release/mongo-server. Writes research/c2-delta-log/results/server-check.json.
"""
import json
import os
import random
import subprocess
import sys
import time

import boto3
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

HARNESS = sys.argv[1]
sys.path.insert(0, HARNESS)
import workload as w  # noqa: E402
from langgraph.checkpoint.mongodb import MongoDBSaver  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PORT = int(os.environ.get("PORT", "27041"))
BUCKET = "c2delta"
DB = "agentck"
OUT = os.path.join(ROOT, "research/c2-delta-log/results/server-check.json")
LOG = os.path.join(ROOT, "research/c2-delta-log/results/server-check-server.log")
ENV = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1",
           AWS_ENDPOINT="http://127.0.0.1:9000", AWS_ALLOW_HTTP="true", FENVIX_SEALED_TPIN_MS="1000", FENVIX_SEALED_E1_WAIT_MS="0", FENVIX_SEALED_TEST_SHORT_E1_WAIT="1")
ARGS = ["--listen", f"127.0.0.1:{PORT}", "--idle-secs", "600", "--holder", "c2delta", "--bucket", BUCKET,
        "--sealed-runs", "2", "--sealed-compactor", "--sealed-drop", "--sealed-derive", "--sealed-codec", "zstd",
        "--customer-key", "checkpoints=thread_id,checkpoint_writes=thread_id"]


def wipe_bucket():
    s3 = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
    b = s3.Bucket(BUCKET)
    if b.creation_date is None:
        b.create()
    else:
        b.objects.all().delete()


def start():
    log = open(LOG, "a")
    p = subprocess.Popen([os.path.join(ROOT, "target/release/mongo-server")] + ARGS, env=ENV, stdout=log, stderr=log)
    for _ in range(100):
        try:
            MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true", serverSelectionTimeoutMS=300).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.2)
    raise SystemExit("server did not start")


def stop(p):
    p.terminate()
    p.wait(60)


def client():
    return MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true")


def raw_docs(c):
    out = {}
    for coll in ("checkpoints", "checkpoint_writes"):
        col = c[DB].get_collection(coll, codec_options=c[DB].codec_options.with_options(document_class=RawBSONDocument))
        out[coll] = [d.raw for d in col.find({}).sort("_id", 1).batch_size(8)]
    return out


def saver_view(saver, graph, threads):
    """What the saver and the graph return per thread (as comparable text)."""
    v = {}
    for t in threads:
        cfg = {"configurable": {"thread_id": t}}
        hist = list(saver.list(cfg))
        latest = saver.get_tuple(cfg)
        old = hist[len(hist) * 3 // 4] if hist else None
        old_t = saver.get_tuple(old.config) if old else None
        st = graph.get_state(cfg)
        v[t] = {
            "history": [repr((h.config, h.checkpoint, h.metadata, h.pending_writes)) for h in hist],
            "latest": repr((latest.config, latest.checkpoint, latest.metadata, latest.pending_writes)) if latest else None,
            "old": repr((old_t.config, old_t.checkpoint, old_t.metadata, old_t.pending_writes)) if old_t else None,
            "state": repr((st.values, st.next)),
        }
    return v


def derived(c):
    return c.admin.command("serverStatus")["fenvix"]["derived"]


def compare(a, b, what, res):
    diff = {k: sum(1 for x, y in zip(a[k], b[k]) if x != y) + abs(len(a[k]) - len(b[k])) for k in a}
    res[what] = {"docs": {k: len(a[k]) for k in a}, "different": diff}
    print(what, res[what], flush=True)
    return sum(diff.values())


def compare_views(a, b, what, res):
    n = sum(1 for t in a for k in a[t] if a[t][k] != b.get(t, {}).get(k))
    res[what] = {"threads": len(a), "different": n}
    print(what, res[what], flush=True)
    return n


def main():
    res = {"args": ARGS}
    open(LOG, "w").close()
    wipe_bucket()
    p = start()
    c = client()
    saver = MongoDBSaver(c, db_name=DB)
    graph = w.build_graph("default", saver)
    plan = [(40, 30), (100, 6)]
    threads = []
    t0 = time.time()
    for n, k in plan:
        for i in range(k):
            tid = f"t{n}-{i:04d}"
            rng = random.Random(f"{w.SEED}-{n}-{i}")
            w.run_thread(graph, tid, w.thread_script(n, rng))
            threads.append(tid)
    res["write_s"] = time.time() - t0
    wrong = 0
    a = raw_docs(c)
    va = saver_view(saver, graph, threads)
    r = c.admin.command("fenvixReseal", DB)
    res["reseal_1"] = {k: v for k, v in r.items() if k != "ok"}
    res["derived_after_reseal_1"] = derived(c)
    print("reseal 1", r, derived(c), flush=True)
    if derived(c)["encoded"] == 0:
        print("NOTHING ENCODED: the reseal wrote no derived document", flush=True)
        wrong += 1
    b = raw_docs(c)
    wrong += compare(a, b, "raw documents before / after the first reseal", res)
    wrong += compare_views(va, saver_view(saver, graph, threads), "saver reads before / after the first reseal", res)
    # more steps on every thread: young rows on top of derived ones
    for tid in threads:
        n = int(tid[1:].split("-")[0])
        i = int(tid.split("-")[1])
        rng = random.Random(f"{w.SEED}-more-{n}-{i}")
        w.run_thread(graph, tid, w.thread_script(10, rng))
    cc = raw_docs(c)
    vc = saver_view(saver, graph, threads)
    r = c.admin.command("fenvixReseal", DB)
    res["reseal_2"] = {k: v for k, v in r.items() if k != "ok"}
    print("reseal 2", r, derived(c), flush=True)
    wrong += compare(cc, raw_docs(c), "raw documents before / after the second reseal (young rows on derived ones)", res)
    wrong += compare_views(vc, saver_view(saver, graph, threads), "saver reads before / after the second reseal", res)
    # the saver deletes some threads; their documents (and so their message rows) go at the next reseal
    gone = threads[::7]
    for t in gone:
        saver.delete_thread(t)
    keep = [t for t in threads if t not in gone]
    before = raw_docs(c)
    vk = {t: vc[t] for t in keep}
    r = c.admin.command("fenvixReseal", DB)
    after = raw_docs(c)
    wrong += compare(before, after, "raw documents after delete_thread, before / after the reseal", res)
    left = sum(c[DB][coll].count_documents({"thread_id": {"$in": gone}}) for coll in ("checkpoints", "checkpoint_writes"))
    res["deleted_threads"] = {"threads": len(gone), "documents_left": left}
    wrong += left
    wrong += compare_views(vk, saver_view(saver, graph, keep), "saver reads of the other threads after delete_thread and reseal", res)
    res["derived_before_restart"] = derived(c)
    # fix round, FIX 3: another app's collection named like the saver's, customer-keyed by user_id, holding documents of
    # the saver's shape and of other shapes, is never rewritten (the encoded counter does not move, bytes unchanged)
    from bson import Binary
    other = c["otherapp"]
    other.create_collection("checkpoints", customerKey="user_id")
    big = bytes([0x93]) + b"".join(bytes([0xd9, 120]) + bytes([97 + i]) * 120 for i in range(3))
    docs = []
    for u in range(20):
        docs.append({"user_id": f"u{u}", "thread_id": f"t{u}", "checkpoint_ns": "", "checkpoint_id": f"c{u}", "type": "msgpack", "checkpoint": Binary(big)})
        docs.append({"user_id": f"u{u}", "kind": "game-save", "state": Binary(big)})
    for d in docs:
        other.checkpoints.insert_one(d)
    other.checkpoints.insert_one({"user_id": "u0", "kind": "tick"})
    col = other.get_collection("checkpoints", codec_options=other.codec_options.with_options(document_class=RawBSONDocument))
    before_o = [d.raw for d in col.find({}).sort("_id", 1).batch_size(8)]
    enc0 = derived(c)["encoded"]
    r = c.admin.command("fenvixReseal", "otherapp")
    after_o = [d.raw for d in col.find({}).sort("_id", 1).batch_size(8)]
    moved = derived(c)["encoded"] - enc0
    res["near_miss_collection"] = {"documents": len(before_o), "resealed": r.get("resealed"), "encoded": moved, "different": sum(1 for x, y in zip(before_o, after_o) if x != y) + abs(len(before_o) - len(after_o))}
    print("near-miss collection (checkpoints keyed by user_id)", res["near_miss_collection"], flush=True)
    wrong += moved + res["near_miss_collection"]["different"]
    # restart on the same bucket: cold reads of the derived runs
    c.close()
    stop(p)
    p = start()
    c = client()
    saver = MongoDBSaver(c, db_name=DB)
    graph = w.build_graph("default", saver)
    wrong += compare(after, raw_docs(c), "raw documents after a restart (cold)", res)
    wrong += compare_views(vk, saver_view(saver, graph, keep), "saver reads after a restart (cold)", res)
    res["derived_after_restart_reads"] = derived(c)
    st = c.admin.command("serverStatus")["fenvix"]
    res["compactor"] = st.get("compactor")
    res["wrong"] = wrong
    c.close()
    stop(p)
    json.dump(res, open(OUT, "w"), indent=1, default=str)
    print("WRONG", wrong)
    sys.exit(1 if wrong else 0)


if __name__ == "__main__":
    try:
        main()
    finally:
        subprocess.run(["pkill", "-f", f"mongo-server --listen 127.0.0.1:{PORT}"])
