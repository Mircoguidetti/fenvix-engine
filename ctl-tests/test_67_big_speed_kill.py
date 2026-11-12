"""P-big-speed (2026-11-12), RULES 8 for the new steps: kill -9 while the customer's box (fix 3), the warm-up before an
upsert (fix 4), the pinned maps and the single-flight fetches (fix 2) and the memory budget (fix 1, `--memory-limit-mb
1024`) are all in use.

Writers save checkpoints and pending writes of many threads with the saver's upserts and record every acknowledged one;
readers at the same time ask each thread's FIRST request (box warm-ups) and NEXT requests. The server is killed with
SIGKILL in the middle, restarted, and then every acknowledged checkpoint and pending write must be there with its value,
and every thread's newest checkpoint must be the newest acknowledged one or a newer unacknowledged one (a write in flight
at the kill may land: MongoDB also may keep an unacknowledged write). Done twice with different kill times. Control: the
same check on a server that was not killed; negative control: with writes acknowledged before they are durable
(`--ack-before-durable --wal-flush-ms 2000`) the same kill must lose acknowledged writes, so the check can fail.
"""
import random
import threading
import time

import pytest
from pymongo.errors import PyMongoError

from lib import Server, make_bucket

FLAGS = ["--memory-limit-mb", "1024"]
SIM = {"FENVIX_SIM_S3": "5,0,10000"}
THREADS = 40


def tid(i):
    return f"kill-u{i:03d}-t0"


def writer(uri_client, w, stop, acked, lock):
    db = uri_client.app
    r = random.Random(w)
    step = {}
    while not stop.is_set():
        i = r.randrange(THREADS)
        t = tid(i)
        s = step.get(t, 0)
        step[t] = s + 1
        cid = f"1ef{w:02d}{s:06d}"
        body = f"{t}/{cid}/" + "x" * r.randrange(200, 3000)
        try:
            db.checkpoints.update_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": cid},
                                      {"$set": {"type": "json", "checkpoint": body, "metadata": {}}}, upsert=True)
            with lock:
                acked["ck"][(t, cid)] = body
            db.checkpoint_writes.update_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": cid, "task_id": "a", "task_path": "", "idx": 0},
                                            {"$setOnInsert": {"value": body[:40]}}, upsert=True)
            with lock:
                acked["w"][(t, cid)] = body[:40]
        except PyMongoError:
            if stop.is_set():
                return
            time.sleep(0.05)


def reader(client, stop, errors):
    db = client.app
    r = random.Random(99)
    while not stop.is_set():
        t = tid(r.randrange(THREADS))
        try:
            d = next(iter(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1)), None)
            if d is not None:
                list(db.checkpoint_writes.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": d["checkpoint_id"]}))
            list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=10))
        except PyMongoError as e:
            if stop.is_set():
                return
            errors.append(repr(e))
            time.sleep(0.05)


def run_and_check(kill_after, kill=True, extra=(), expect_loss=False):
    s = Server(make_bucket("bskill"), "bskill", idle=600, extra=FLAGS + list(extra), env=SIM).start()
    acked = {"ck": {}, "w": {}}
    lock = threading.Lock()
    stop = threading.Event()
    errors = []
    c = s.client()
    c.app.checkpoints.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1)], unique=True)
    c.app.checkpoint_writes.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1), ("task_id", 1), ("idx", 1)], unique=True)
    clients = [s.client(retryWrites=False) for _ in range(5)]
    ths = [threading.Thread(target=writer, args=(clients[w], w, stop, acked, lock)) for w in range(3)]
    ths += [threading.Thread(target=reader, args=(clients[3 + k], stop, errors)) for k in range(2)]
    for th in ths:
        th.start()
    time.sleep(kill_after)
    if kill:
        s.kill9()
    stop.set()
    for th in ths:
        th.join(30)
    with lock:
        ck, ws = dict(acked["ck"]), dict(acked["w"])
    if kill:
        s.start()
    else:
        s.term()
        s.start()
    try:
        c = s.client()
        db = c.app
        got = {(d["thread_id"], d["checkpoint_id"]): d["checkpoint"] for d in db.checkpoints.find({})}
        gotw = {(d["thread_id"], d["checkpoint_id"]): d["value"] for d in db.checkpoint_writes.find({})}
        missing = [k for k in ck if k not in got]
        differ = [k for k in ck if k in got and got[k] != ck[k]]
        missing_w = [k for k in ws if k not in gotw]
        differ_w = [k for k in ws if k in gotw and gotw[k] != ws[k]]
        # the newest checkpoint of every thread through the index (the customer's box path after the restart)
        stale = []
        for i in range(THREADS):
            t = tid(i)
            mine = sorted(cid for (tt, cid) in ck if tt == t)
            if not mine:
                continue
            d = next(iter(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1)))
            if d["checkpoint_id"] < mine[-1]:
                stale.append((t, d["checkpoint_id"], mine[-1]))
        box = c.admin.command("serverStatus")["fenvix"]["customerBox"]
        print(f"kill={kill} after {kill_after}s: acknowledged {len(ck)} checkpoints, {len(ws)} writes; missing {len(missing)}, differ {len(differ)},"
              f" writes missing {len(missing_w)}, differ {len(differ_w)}, stale newest {len(stale)}; reader errors {len(errors)}; box {box}")
        assert len(ck) > 50, "too few acknowledged writes: the test did not exercise the writes"
        if expect_loss:
            assert missing or missing_w, "negative control: acknowledging before durability lost nothing (the check could not fail)"
            return
        assert not missing and not differ and not missing_w and not differ_w and not stale, (missing[:3], differ[:3], missing_w[:3], differ_w[:3], stale[:3])
    finally:
        s.term()


@pytest.mark.parametrize("kill_after", [7, 13])
def test_kill9_loses_no_acknowledged_write(kill_after):
    run_and_check(kill_after, kill=True)


def test_control_without_kill():
    run_and_check(6, kill=False)


def test_negative_control_ack_before_durable_loses_writes():
    """Writes acknowledged before they are durable (flushed every 2 s): the kill must lose some, or the check is blind."""
    run_and_check(9, kill=True, extra=["--wal-flush-ms", "2000", "--ack-before-durable"], expect_loss=True)
