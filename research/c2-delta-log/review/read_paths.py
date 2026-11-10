"""Review of c2-delta-log, check 1 at the server: every read path returns derived documents byte for byte.

The same LangGraph-saver-shaped documents (BSON with a msgpack `checkpoint` / `value` binary whose message list is an
array of msgpack ext values, as langgraph-checkpoint-mongodb 0.5.1 writes them) go to mongod 7.0 and to mongo-server
(sealed runs, compactor, drop, `--sealed-derive`, zstd, both collections customer-keyed by `thread_id`). After
`fenvixReseal` (every thread becomes message rows plus derived documents) every read path is compared, raw BSON:
find by _id (the locator), find by thread (latest, whole history, ranges with sort and limit), the whole collection,
aggregation ($match, $group with $binarySize, $sort, $project), count and distinct, an update of a derived document
read back through a change stream (fullDocument updateLookup), reads inside a transaction, and all of it again after a
restart (cold runs). Usage: read_paths.py (needs MinIO on 9000, mongod on MONGOD_PORT, target/release/mongo-server).
"""
import json, os, random, subprocess, sys, time
import boto3
from bson import ObjectId, Binary
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo import MongoClient

ROOT = os.environ.get("ROOT") or os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
PORT = int(os.environ.get("PORT", "27051"))
MPORT = int(os.environ.get("MONGOD_PORT", "27052"))
BUCKET = "rvreads"
DB = "agentdb"
OUT = os.environ.get("OUT", os.path.join(ROOT, "research/c2-delta-log/review/results/read-paths.json"))
ENV = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1",
           AWS_ENDPOINT="http://127.0.0.1:9000", AWS_ALLOW_HTTP="true", FENVIX_SEALED_TPIN_MS="30000", FENVIX_SEALED_E1_WAIT_MS="0", FENVIX_SEALED_TEST_SHORT_E1_WAIT="1")
ARGS = ["--listen", f"127.0.0.1:{PORT}", "--idle-secs", "600", "--holder", "rvreads", "--bucket", BUCKET,
        "--sealed-runs", "2", "--sealed-compactor", "--sealed-drop", "--sealed-derive", "--sealed-codec", "zstd",
        "--customer-key", "checkpoints=thread_id,checkpoint_writes=thread_id", "--replica-set", "rs0", "--advertise", f"127.0.0.1:{PORT}"]
OPTS = os.environ.get("EXTRA_ARGS", "").split()
if os.environ.get("NODERIVE"):  # control: the same check with derived documents off and lz4
    ARGS = [a for a in ARGS if a not in ("--sealed-derive",)]
    ARGS[ARGS.index("zstd")] = "lz4"


def wipe():
    s3 = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
    b = s3.Bucket(BUCKET)
    if b.creation_date is None:
        b.create()
    else:
        b.objects.all().delete()


def start():
    try:  # a server left on the port would answer instead of ours
        MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true", serverSelectionTimeoutMS=300).admin.command("ping")
        raise SystemExit(f"port {PORT} is already served: stop that server first")
    except SystemExit:
        raise
    except Exception:
        pass
    log = open(OUT.replace(".json", "-server.log"), "a")
    p = subprocess.Popen([os.path.join(ROOT, "target/release/mongo-server")] + ARGS + OPTS, env=ENV, stdout=log, stderr=log)
    for _ in range(150):
        try:
            MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true", serverSelectionTimeoutMS=300).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.2)
    raise SystemExit("server did not start")


def stop(p):
    p.terminate()
    p.wait(60)


def mp_str(s):
    b = s.encode()
    if len(b) < 32:
        return bytes([0xa0 | len(b)]) + b
    if len(b) < 256:
        return bytes([0xd9, len(b)]) + b
    return bytes([0xda]) + len(b).to_bytes(2, "big") + b


def message(rng, tag):
    words = ["the ", "agent ", "tool ", "returns ", "order ", "flight ", "\n", "def f(x):", " return x", "user "]
    size = rng.choice([rng.randint(5, 30), rng.randint(80, 900), rng.randint(80, 900), rng.randint(2000, 6000)])
    text = "".join(rng.choice(words) for _ in range(size // 4 + 1))[:size]
    kind = ["HumanMessage", "AIMessage", "ToolMessage"][tag % 3]
    inner = b"\x94" + mp_str("langchain_core.messages") + mp_str(kind) + b"\x82" + mp_str("id") + mp_str(f"run-{tag:016x}") + mp_str("content") + mp_str(text)
    if len(inner) < 256:
        return bytes([0xc7, len(inner), 5]) + inner
    return bytes([0xc8]) + len(inner).to_bytes(2, "big") + b"\x05" + inner


def arr(items):
    h = bytes([0x90 | len(items)]) if len(items) < 16 else b"\xdc" + len(items).to_bytes(2, "big")
    return h + b"".join(items)


def gen(n_threads, steps, seed=11):
    rng = random.Random(seed)
    ck, wr = [], []
    tag = 0
    for t in range(n_threads):
        tid = f"thread-{t:04d}"
        msgs = []
        for s in range(1, steps + 1):
            new = []
            for _ in range(rng.randint(1, 3)):
                tag += 1
                new.append(message(rng, tag))
            msgs += new
            if t % 4 == 1 and len(msgs) > 12:  # summarising agent
                tag += 1
                msgs = [message(rng, tag)] + msgs[-2:]
            cid = f"1f0{t:04d}-{s:06d}"
            cp = b"\x84" + mp_str("v") + b"\x04" + mp_str("id") + mp_str(cid) + mp_str("channel_values") + b"\x81" + mp_str("messages") + arr(msgs) + mp_str("channel_versions") + b"\x81" + mp_str("messages") + mp_str(f"{s:032d}")
            # filter fields in varying order, as the saver's upserts write them
            first = [("thread_id", tid), ("checkpoint_ns", ""), ("checkpoint_id", cid)]
            rng.shuffle(first)
            d = SON([("_id", ObjectId())] + first + [("checkpoint", Binary(cp)), ("metadata", SON([("source", ["msgpack", Binary(mp_str("loop"))]), ("step", ["msgpack", Binary(bytes([s % 100]))]), ("parents", SON())])),
                     ("parent_checkpoint_id", f"1f0{t:04d}-{s - 1:06d}" if s > 1 else None), ("type", "msgpack")])
            ck.append(d)
            w = SON([("_id", ObjectId()), ("thread_id", tid), ("checkpoint_ns", ""), ("checkpoint_id", cid), ("task_id", f"task-{s}"), ("task_path", "~__pregel_pull, agent"), ("idx", 0),
                     ("channel", "messages"), ("type", "msgpack"), ("value", Binary(arr(new)))])
            wr.append(w)
    return ck, wr


def raw(it):
    return [bytes(d.raw) for d in it]


def reads(c, threads, seed):
    """Every read path; returns {path: list of raw BSON lists}."""
    rng = random.Random(seed)
    db = c.get_database(DB, codec_options=c.codec_options.with_options(document_class=RawBSONDocument))
    out = {}
    ckc, wrc = db.checkpoints, db.checkpoint_writes
    ids = [d["_id"] for d in c[DB].checkpoints.find({}, {"_id": 1}).sort("_id", 1)]
    wids = [d["_id"] for d in c[DB].checkpoint_writes.find({}, {"_id": 1}).sort("_id", 1)]
    out["find_by_id"] = [raw(ckc.find({"_id": i})) for i in ids] + [raw(wrc.find({"_id": i})) for i in wids]
    out["latest"] = [raw(ckc.find({"thread_id": t, "checkpoint_ns": ""}).sort("checkpoint_id", -1).limit(1)) for t in threads]
    out["history"] = [raw(ckc.find({"thread_id": t}).sort("checkpoint_id", 1).batch_size(8)) for t in threads]
    out["writes_of_step"] = []
    out["range"] = []
    for t in threads:
        s = rng.randint(2, 30)
        cid = f"1f0{int(t[-4:]):04d}-{s:06d}"
        out["range"].append(raw(ckc.find({"thread_id": t, "checkpoint_id": {"$lt": cid}}).sort("checkpoint_id", -1).limit(5)))
        out["writes_of_step"].append(raw(wrc.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": cid}).sort([("task_id", 1), ("idx", 1)])))
    out["whole_ck"] = [raw(ckc.find({}).sort("_id", 1).batch_size(8))]
    out["whole_wr_desc"] = [raw(wrc.find({}).sort("_id", -1).batch_size(16))]
    out["agg"] = [raw(ckc.aggregate([{"$match": {"thread_id": {"$in": threads[:10]}}}, {"$group": {"_id": "$thread_id", "n": {"$sum": 1}, "b": {"$sum": {"$binarySize": "$checkpoint"}}, "last": {"$max": "$checkpoint_id"}}}, {"$sort": {"_id": 1}}])),
                  raw(ckc.aggregate([{"$match": {"thread_id": threads[3]}}, {"$sort": {"checkpoint_id": -1}}, {"$limit": 3}, {"$project": {"checkpoint": 1, "type": 1}}])),
                  raw(wrc.aggregate([{"$match": {"thread_id": {"$in": threads[:5]}}}, {"$lookup": {"from": "checkpoints", "localField": "checkpoint_id", "foreignField": "checkpoint_id", "as": "ck"}}, {"$sort": {"_id": 1}}]))]
    out["count_distinct"] = [[ckc.count_documents({}).to_bytes(8, "little"), wrc.count_documents({"thread_id": threads[1]}).to_bytes(8, "little")], [str(sorted(c[DB].checkpoints.distinct("thread_id"))).encode()]]
    with c.start_session() as s:
        with s.start_transaction():
            out["txn"] = [raw(ckc.find({"_id": i}, session=s)) for i in ids[::7]] + [raw(ckc.find({"thread_id": threads[2]}, session=s).sort("checkpoint_id", 1))]
    return out


def compare(a, b):
    diff = {}
    n = 0
    for k in a:
        for x, y in zip(a[k], b[k]):
            n += max(len(x), 1)
            if x != y:
                diff[k] = diff.get(k, 0) + 1
        if len(a[k]) != len(b[k]):
            diff[k] = diff.get(k, 0) + 1
    return n, diff


def derived(c):
    return c.admin.command("serverStatus").get("fenvix", {}).get("derived", {})


def main():
    res = {}
    n_threads, steps = int(os.environ.get("THREADS", "24")), int(os.environ.get("STEPS", "40"))
    ck, wr = gen(n_threads, steps)
    threads = sorted({d["thread_id"] for d in ck})
    m = MongoClient(f"mongodb://127.0.0.1:{MPORT}/?directConnection=true", replicaset=None)
    m.drop_database(DB)
    wipe()
    p = start()
    us = MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true")
    for c in (m, us):
        for coll, docs in (("checkpoints", ck), ("checkpoint_writes", wr)):
            for i in range(0, len(docs), 50):
                c[DB][coll].insert_many(docs[i:i + 50], ordered=True)
    r = us.admin.command({"fenvixReseal": DB})
    res["reseal"] = {k: v for k, v in r.items() if k != "ok"}
    res["derived_after_reseal"] = derived(us)
    want = reads(m, threads, 1)
    got = reads(us, threads, 1)
    res["warm"] = compare(want, got)
    # an update of derived documents, seen by a change stream (updateLookup reads the document back)
    events = {}
    for name, c in (("mongod", m), ("ours", us)):
        try:
            with c[DB].checkpoints.watch(full_document="updateLookup", max_await_time_ms=2000) as cs:
                for t in threads[:6]:
                    c[DB].checkpoints.update_one({"thread_id": t, "checkpoint_id": f"1f0{int(t[-4:]):04d}-{5:06d}"}, {"$set": {"metadata.note": "edited"}})
                evs = []
                deadline = time.time() + 20
                while len(evs) < 6 and time.time() < deadline:
                    e = cs.try_next()
                    if e is not None:
                        evs.append((e["operationType"], bytes(RawBSONDocument(__import__("bson").encode(e["fullDocument"])).raw)))
                events[name] = evs
        except Exception as e:  # change streams may be unsupported on one side: report it
            events[name] = f"error: {e}"
    res["change_stream"] = {"mongod_events": len(events["mongod"]) if isinstance(events["mongod"], list) else events["mongod"],
                            "ours_events": len(events["ours"]) if isinstance(events["ours"], list) else events["ours"],
                            "equal": events["mongod"] == events["ours"]}
    r = us.admin.command({"fenvixReseal": DB})
    want = reads(m, threads, 2)
    got = reads(us, threads, 2)
    res["after_update_and_reseal"] = compare(want, got)
    stop(p)
    p = start()
    us = MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true")
    got = reads(us, threads, 2)
    res["cold_restart"] = compare(want, got)
    # fix check: a cold restart, a write to another collection, a reseal before any saver write: still derived
    stop(p)
    p = start()
    us = MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true")
    us[DB].other.insert_one({"x": 1})
    m[DB].other.insert_one({"x": 1})
    r = us.admin.command({"fenvixReseal": DB})
    res["restart_reseal"] = {"reseal": {k: v for k, v in r.items() if k in ("resealed", "encoded")}, "derived": derived(us)}
    got = reads(us, threads, 2)
    res["restart_reseal_reads"] = compare(want, got)
    res["derived_end"] = derived(us)
    stop(p)
    res["docs"] = {"checkpoints": len(ck), "checkpoint_writes": len(wr), "threads": len(threads)}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(res, open(OUT, "w"), indent=1, default=str)
    print(json.dumps(res, default=str))
    bad = [k for k in ("warm", "after_update_and_reseal", "cold_restart", "restart_reseal_reads") if res[k][1]] + ([] if res["change_stream"]["equal"] else ["change_stream"])
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    try:
        main()
    finally:
        subprocess.run(["pkill", "-f", f"mongo-server --listen 127.0.0.1:{PORT}"])
