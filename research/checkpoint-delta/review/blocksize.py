"""Thread message log with 64 KiB message blocks (today's run block size) instead of 256 KiB, zstd and lz4,
on the owner's scripted runs (all threads). -> results/blocksize.json"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lz4.frame, psycopg
from psycopg.rows import dict_row
from langgraph.checkpoint.postgres import PostgresSaver
import options as o
MIX = {10: 0.40, 40: 0.35, 100: 0.20, 300: 0.05}
res = {}
for n in MIX:
    conn = psycopg.connect(o.PG + f"ck_default_{n}", autocommit=True, prepare_threshold=0, row_factory=dict_row)
    s = PostgresSaver(conn); serde = s.serde
    tids = [r["thread_id"] for r in conn.execute("select distinct thread_id from checkpoints order by 1")]
    tot = {}
    for t in tids:
        recs = o.records_for_thread(s, t)
        for blk in (256 * 1024, 64 * 1024):
            for cname, c in (("zstd", o.z), ("lz4", lambda b: lz4.frame.compress(b))):
                o.LOG_BLOCK, zs, o.z = blk, o.z, c
                try:
                    b, _, sk = o.log_encode(serde, recs)
                finally:
                    o.z = zs
                k = f"log_{blk//1024}k_{cname}"
                tot[k] = tot.get(k, 0) + sum(map(len, b)) + sum(map(len, sk[0]))
    res[n] = {k: v / len(tids) for k, v in tot.items()}
    print(n, res[n], flush=True)
res["mix"] = {k: sum(MIX[n] * res[n][k] for n in MIX) for k in res[10]}
print("mix", res["mix"])
json.dump(res, open("results/blocksize.json", "w"), indent=1)
