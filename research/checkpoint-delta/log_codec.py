"""The thread message log with the codec our run format uses today (lz4 frames per block)
instead of zstd, and an ESTIMATE of key overhead. Same records and layout as options.py.

Usage: log_codec.py N [max_threads] -> results/options/N-log-codec.json
"""
import json, os, sys
import lz4.frame
import psycopg
from psycopg.rows import dict_row
from langgraph.checkpoint.postgres import PostgresSaver
import options as o

def main():
    n = int(sys.argv[1]); mx = int(sys.argv[2]) if len(sys.argv) > 2 else 10**9
    conn = psycopg.connect(o.PG + f"ck_default_{n}", autocommit=True, prepare_threshold=0, row_factory=dict_row)
    saver = PostgresSaver(conn); serde = saver.serde
    tids = [r["thread_id"] for r in conn.execute("select distinct thread_id from checkpoints order by 1")][:mx]
    tot = {"log_zstd": 0, "log_lz4": 0}
    for tid in tids:
        recs = o.records_for_thread(saver, tid)
        for name, codec in (("log_zstd", o.z), ("log_lz4", lambda b: lz4.frame.compress(b))):
            o.z_saved = o.z
            o.z = codec
            try:
                blocks, where, sks = o.log_encode(serde, recs)
            finally:
                o.z = o.z_saved
            tot[name] += sum(map(len, blocks)) + sum(map(len, sks[0]))
    out = {"n": n, "threads": len(tids), "bytes_per_thread": {k: v / len(tids) for k, v in tot.items()}}
    json.dump(out, open(os.path.join(o.HERE, "results", "options", f"{n}-log-codec.json"), "w"), indent=1)
    print(json.dumps(out))

main()
