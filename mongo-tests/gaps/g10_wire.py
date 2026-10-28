"""Item 10: wire protocol: OP_COMPRESSED (zlib) negotiated through hello, and the shape of the hello reply."""
from pymongo import MongoClient
from gaps_py import case


@case("g10.wire.compression_zlib", sa=True)
def _zlib(client, db):
    host, port = client.address
    c = MongoClient(host, port, directConnection=True, compressors="zlib")
    try:
        h = c.admin.command({"hello": 1, "compression": ["zlib", "bogus"]})
        t = c[db.name].t
        t.insert_many([{"_id": i, "s": "x" * 200} for i in range(50)])
        got = list(t.find({}, {"_id": 1}).sort("_id", 1).limit(5))
        return {"negotiated": h.get("compression"), "docs": got, "count": t.count_documents({}), "agg": list(t.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}]))}
    finally:
        c.close()


def keys(h):
    return [k for k in h if k not in ("$clusterTime", "operationTime")]


@case("g10.wire.hello_shape_sa", sa=True)
def _hs(client, db):
    h = client.admin.command("hello")
    i = client.admin.command("isMaster")
    j = client.admin.command({"isMaster": 1, "helloOk": True})
    k = client.admin.command({"hello": 1, "compression": ["zlib", "zstd", "snappy"]})
    return {"hello": keys(h), "ismaster": keys(i), "ismaster_helloOk": keys(j), "compression": keys(k)[:2], "tv": sorted(h["topologyVersion"]), "tv_counter": int(h["topologyVersion"]["counter"])}


@case("g10.wire.hello_shape_rs", rs=True)
def _hsr(client, db):
    h = client.admin.command("hello")
    return {"hello": keys(h), "lastWrite": sorted(h["lastWrite"]), "tv": sorted(h["topologyVersion"])}


@case("g10.wire.awaitable_hello", sa=True)
def _await(client, db):
    import time
    h = client.admin.command("hello")
    t0 = time.time()
    h2 = client.admin.command({"hello": 1, "topologyVersion": h["topologyVersion"], "maxAwaitTimeMS": 400})
    return {"waited": 0.3 < time.time() - t0 < 2.0, "same_tv": h2["topologyVersion"] == h["topologyVersion"]}
