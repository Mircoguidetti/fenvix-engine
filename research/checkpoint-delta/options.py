"""Our storage options, computed on the real checkpoints of the default-mode run.

Logical record of step k = what the app gets back for that checkpoint, in the saver's own
encoding: serde.dumps_typed({"checkpoint" (with every channel value), "metadata",
"writes" (the pending writes)}). Read back from the default-mode Postgres database with
PostgresSaver.list, oldest first.

Options (zstd level 3 everywhere):
  full         each record compressed alone.
  bin          binary delta: record k compressed with record k-1 as a raw-content dictionary.
  json         structure-aware delta: every message is serialised once per thread and kept in a
               message store; a record becomes a small skeleton with references, and the
               messages list is "the previous list plus these new references". Each delta is
               compressed with the previous delta as dictionary (the skeletons are alike).
  bin_K / json_K  the same with a full record every K steps (K = 10, 50): step k needs at most
               K-1 deltas after its full record.
  log          thread message log: every distinct message once per thread, in order of first
               appearance, cut in blocks of 256 KiB raw and compressed; each checkpoint is a
               skeleton pointing into the log; skeletons are packed in blocks of 64 KiB raw, as rows
               are in a run. Any step is read directly (no chain): its skeleton block plus the log
               blocks that hold its messages.

Read cost per option, for the latest step and an old step (k = N/4): bytes fetched (sum of the
compressed objects needed) and CPU time to rebuild the Python checkpoint (decompress, apply,
deserialise), median of 3. Every rebuild is checked byte for byte against the logical record.

Usage: options.py N [max_threads]   ->  results/options/N.json
"""
import hashlib
import json
import os
import statistics
import sys
import time

import ormsgpack
import psycopg
import zstandard as zstd
from psycopg.rows import dict_row
from langchain_core.messages import BaseMessage
from langgraph.checkpoint.postgres import PostgresSaver

PG = "postgresql://postgres:pg@127.0.0.1:5432/"
HERE = os.path.dirname(os.path.abspath(__file__))
LEVEL = 3
LOG_BLOCK = 256 * 1024
SK_BLOCK = 64 * 1024
WINDOW = 27
KS = (10, 50)


def records_for_thread(saver, tid):
    tups = list(saver.list({"configurable": {"thread_id": tid}}))[::-1]
    out = []
    for t in tups:
        obj = {"checkpoint": t.checkpoint, "metadata": t.metadata, "writes": [list(w) for w in (t.pending_writes or [])]}
        out.append(obj)
    return out


# ---------------------------------------------------------------- binary
def cparams():
    return zstd.ZstdCompressionParameters.from_level(LEVEL, window_log=WINDOW)


def z(b):
    return zstd.ZstdCompressor(compression_params=cparams()).compress(b)


def unz(b):
    return zstd.ZstdDecompressor(max_window_size=2**WINDOW).decompress(b)


def zd(b, prev):
    d = zstd.ZstdCompressionDict(prev, dict_type=zstd.DICT_TYPE_RAWCONTENT)
    return zstd.ZstdCompressor(dict_data=d, compression_params=cparams()).compress(b)


def unzd(b, prev):
    d = zstd.ZstdCompressionDict(prev, dict_type=zstd.DICT_TYPE_RAWCONTENT)
    return zstd.ZstdDecompressor(dict_data=d, max_window_size=2**WINDOW).decompress(b)


# ---------------------------------------------------------------- structure-aware
class MsgStore:
    def __init__(self):
        self.index, self.blobs = {}, []

    def ref(self, serde, m, new):
        b = serde.dumps_typed(m)[1]
        h = hashlib.blake2b(b, digest_size=16).digest()
        i = self.index.get(h)
        if i is None:
            i = self.index[h] = len(self.blobs)
            self.blobs.append(b)
            new.append(b)
        return i


def skeleton(serde, obj, store, prev_msgs, new):
    """Replace every message by {"$r": i}; the messages channel by {"$p": n, "t": [...]} when the
    previous checkpoint's list is a prefix. Returns (skeleton_obj, this_msgs_refs)."""

    def walk(o):
        if isinstance(o, BaseMessage):
            return {"$r": store.ref(serde, o, new)}
        if isinstance(o, list):
            return [walk(x) for x in o]
        if isinstance(o, tuple):
            return {"$t": [walk(x) for x in o]}
        if isinstance(o, dict):
            return {k: walk(v) for k, v in o.items()}
        return o

    cp = dict(obj["checkpoint"])
    cv = dict(cp["channel_values"])
    refs = None
    if isinstance(cv.get("messages"), list):
        refs = [store.ref(serde, m, new) for m in cv["messages"]]
        if prev_msgs is not None and refs[: len(prev_msgs)] == prev_msgs:
            cv["messages"] = {"$p": len(prev_msgs), "t": refs[len(prev_msgs):]}
        else:
            cv["messages"] = {"$f": refs}
    cp["channel_values"] = {k: (v if k == "messages" else walk(v)) for k, v in cv.items()}
    sk = {"checkpoint": cp, "metadata": walk(obj["metadata"]), "writes": walk(obj["writes"])}
    # the skeleton is msgpack via the saver serde, so non-JSON values (versions, sets) survive
    return sk, refs


def unskeleton(serde, sk, blobs, prev_msgs, cache):
    def msg(i):
        m = cache.get(i)
        if m is None:
            m = cache[i] = serde.loads_typed(("msgpack", blobs[i]))
        return m

    def walk(o):
        if isinstance(o, dict):
            if "$r" in o and len(o) == 1:
                return msg(o["$r"])
            if "$t" in o and len(o) == 1:
                return tuple(walk(x) for x in o["$t"])
            return {k: walk(v) for k, v in o.items()}
        if isinstance(o, list):
            return [walk(x) for x in o]
        return o

    cp = dict(sk["checkpoint"])
    cv = dict(cp["channel_values"])
    refs = None
    if "messages" in cv and isinstance(cv["messages"], dict):
        mm = cv["messages"]
        refs = prev_msgs[: mm["$p"]] + mm["t"] if "$p" in mm else mm["$f"]
        cv["messages"] = [msg(i) for i in refs]
    cp["channel_values"] = {k: (v if k == "messages" else walk(v)) for k, v in cv.items()}
    obj = {"checkpoint": cp, "metadata": walk(sk["metadata"]), "writes": walk(sk["writes"])}
    return obj, refs


def json_encode_chain(serde, recs, K):
    """Encoded deltas; a full record (store reset) at k % K == 0 (K=None: only k=0)."""
    out, store, prev, prev_payload = [], None, None, None
    for k, r in enumerate(recs):
        full = K is None and k == 0 or K is not None and k % K == 0
        if full:
            store, prev = MsgStore(), None
        new = []
        sk, prev = skeleton(serde, r, store, prev, new)
        payload = ormsgpack.packb([serde.dumps_typed(sk)[1], new])
        out.append(z(payload) if full else zd(payload, prev_payload))
        prev_payload = payload
    return out


def json_rebuild(serde, enc, k, K):
    base = 0 if K is None else (k // K) * K
    blobs, prev, cache, obj, payload = [], None, {}, None, None
    for j in range(base, k + 1):
        payload = unz(enc[j]) if j == base else unzd(enc[j], payload)
        skb, new = ormsgpack.unpackb(payload)
        blobs.extend(new)
        if j == k:
            obj, _ = unskeleton(serde, serde.loads_typed(("msgpack", skb)), blobs, prev, cache)
        else:
            # only the reference list is needed from intermediate skeletons
            sk = serde.loads_typed(("msgpack", skb))
            mm = sk["checkpoint"]["channel_values"].get("messages")
            if isinstance(mm, dict):
                prev = prev[: mm["$p"]] + mm["t"] if "$p" in mm else mm["$f"]
    return obj, sum(len(enc[j]) for j in range(base, k + 1))


def bin_encode_chain(raws, K):
    out = []
    for k, b in enumerate(raws):
        full = k == 0 if K is None else k % K == 0
        out.append(z(b) if full else zd(b, raws[k - 1]))
    return out


def bin_rebuild(enc, k, K):
    base = 0 if K is None else (k // K) * K
    cur = unz(enc[base])
    for j in range(base + 1, k + 1):
        cur = unzd(enc[j], cur)
    return cur, sum(len(enc[j]) for j in range(base, k + 1))


# ---------------------------------------------------------------- thread message log
def log_encode(serde, recs):
    store, raw_sk = MsgStore(), []
    for r in recs:
        sk, _ = skeleton(serde, r, store, None, [])  # full ref list: no chain between skeletons
        raw_sk.append(serde.dumps_typed(sk)[1])
    # skeleton blocks: skels[k] = (block id, position); sk_blocks = compressed blocks
    sk_blocks, skels, cur, cur_len = [], [], [], 0
    for b in raw_sk:
        if cur and cur_len + len(b) > SK_BLOCK:
            sk_blocks.append(z(ormsgpack.packb(cur)))
            cur, cur_len = [], 0
        skels.append((len(sk_blocks), len(cur)))
        cur.append(b)
        cur_len += len(b)
    if cur:
        sk_blocks.append(z(ormsgpack.packb(cur)))
    # cut the log into blocks of LOG_BLOCK raw bytes; remember which block holds each message
    blocks, where, cur, cur_len = [], [], [], 0
    for i, b in enumerate(store.blobs):
        if cur and cur_len + len(b) > LOG_BLOCK:
            blocks.append(z(ormsgpack.packb(cur)))
            cur, cur_len = [], 0
        where.append((len(blocks), len(cur)))
        cur.append(b)
        cur_len += len(b)
    if cur:
        blocks.append(z(ormsgpack.packb(cur)))
    return blocks, where, (sk_blocks, skels)


def _refs_in(o, acc):
    if isinstance(o, dict):
        if "$r" in o and len(o) == 1:
            acc.add(o["$r"])
        for v in o.values():
            _refs_in(v, acc)
    elif isinstance(o, list):
        for v in o:
            _refs_in(v, acc)


def log_rebuild(serde, blocks, where, sks, k):
    sk_blocks, skels = sks
    bid, pos = skels[k]
    skz = sk_blocks[bid]
    sk = serde.loads_typed(("msgpack", ormsgpack.unpackb(unz(skz))[pos]))
    need = set()
    mm = sk["checkpoint"]["channel_values"].get("messages")
    if isinstance(mm, dict):
        need.update(mm["$f"])
    _refs_in({"m": sk["metadata"], "w": sk["writes"], "c": sk["checkpoint"]}, need)
    need.discard(None)
    bids = sorted({where[i][0] for i in need if isinstance(i, int)})
    got = {b: ormsgpack.unpackb(unz(blocks[b])) for b in bids}
    blobs = {i: got[where[i][0]][where[i][1]] for i in need if isinstance(i, int)}

    class L(dict):
        def __getitem__(self, i):
            return dict.__getitem__(self, i)

    obj, _ = unskeleton(serde, sk, L(blobs), None, {})
    return obj, len(skz) + sum(len(blocks[b]) for b in bids)


# ---------------------------------------------------------------- driver
def timed(fn, reps=3):
    ts, out = [], None
    for _ in range(reps):
        t = time.perf_counter()
        out = fn()
        ts.append(time.perf_counter() - t)
    return out, statistics.median(ts)


def main():
    n = int(sys.argv[1])
    max_threads = int(sys.argv[2]) if len(sys.argv) > 2 else 10**9
    read_threads = 4
    conn = psycopg.connect(PG + f"ck_default_{n}", autocommit=True, prepare_threshold=0, row_factory=dict_row)
    saver = PostgresSaver(conn)
    serde = saver.serde
    tids = [r["thread_id"] for r in conn.execute("select distinct thread_id from checkpoints order by 1")][:max_threads]

    tot = {k: 0 for k in ["logical", "full", "bin", "json", "log"] + [f"{o}_{K}" for o in ("bin", "json") for K in KS]}
    reads = {}
    mismatches = 0
    checked = 0
    t_start = time.time()
    for ti, tid in enumerate(tids):
        recs = records_for_thread(saver, tid)
        assert len(recs) == n, (tid, len(recs))
        raws = [serde.dumps_typed(r)[1] for r in recs]
        tot["logical"] += sum(map(len, raws))
        full = [z(b) for b in raws]
        tot["full"] += sum(map(len, full))
        enc = {}
        enc["bin"] = bin_encode_chain(raws, None)
        enc["json"] = json_encode_chain(serde, recs, None)
        for K in KS:
            enc[f"bin_{K}"] = bin_encode_chain(raws, K)
            enc[f"json_{K}"] = json_encode_chain(serde, recs, K)
        for name, e in enc.items():
            tot[name] += sum(map(len, e))
        blocks, where, skels = log_encode(serde, recs)
        tot["log"] += sum(map(len, blocks)) + sum(map(len, skels[0]))

        if ti < read_threads:
            for label, k in (("latest", n - 1), ("old", n // 4)):
                res = {}
                o, t = timed(lambda: serde.loads_typed(("msgpack", unz(full[k]))))
                res["full"] = (len(full[k]), t)
                for name in enc:
                    K = None if "_" not in name else int(name.split("_")[1])
                    if name.startswith("bin"):
                        (b, nbytes), t = timed(lambda: (lambda bb: (serde.loads_typed(("msgpack", bb[0])), bb[1]))(bin_rebuild(enc[name], k, K)))
                        ok = serde.dumps_typed(b)[1] == raws[k]
                    else:
                        (b, nbytes), t = timed(lambda: json_rebuild(serde, enc[name], k, K))
                        ok = serde.dumps_typed(b)[1] == raws[k]
                    checked += 1
                    mismatches += not ok
                    res[name] = (nbytes, t)
                (b, nbytes), t = timed(lambda: log_rebuild(serde, blocks, where, skels, k))
                checked += 1
                mismatches += serde.dumps_typed(b)[1] != raws[k]
                res["log"] = (nbytes, t)
                res["logical"] = (len(raws[k]), None)
                for name, (nb, t) in res.items():
                    reads.setdefault(label, {}).setdefault(name, []).append((nb, t))
        print(f"N={n} thread {ti+1}/{len(tids)} {time.time()-t_start:.0f}s", file=sys.stderr, flush=True)

    summary = {
        "n": n,
        "threads": len(tids),
        "bytes_total": tot,
        "bytes_per_thread": {k: v / len(tids) for k, v in tot.items()},
        "read": {
            label: {
                name: {
                    "bytes_mean": statistics.mean(x[0] for x in v),
                    "ms_median": None if v[0][1] is None else 1000 * statistics.median(x[1] for x in v),
                }
                for name, v in d.items()
            }
            for label, d in reads.items()
        },
        "read_threads": min(read_threads, len(tids)),
        "rebuild_checked": checked,
        "rebuild_mismatches": mismatches,
    }
    os.makedirs(os.path.join(HERE, "results", "options"), exist_ok=True)
    json.dump(summary, open(os.path.join(HERE, "results", "options", f"{n}.json"), "w"), indent=1)
    print(json.dumps({k: summary[k] for k in ("n", "threads", "bytes_per_thread", "rebuild_mismatches")}, indent=1))


if __name__ == "__main__":
    main()
