"""Read concern, write concern and read preference as seen by the server (single-node replica set on the real side)."""
from bson import Timestamp
from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import Primary, PrimaryPreferred, Secondary, SecondaryPreferred, Nearest
from probe_diff import case


def seed(db):
    db.t.insert_many([{"_id": i, "v": i} for i in range(5)])


def rc_probe(level):
    def f(client, db):
        seed(db)
        c = db.get_collection("t", read_concern=ReadConcern(level))
        return {"find": list(c.find({}, {"_id": 1}).sort("_id", 1)), "count": c.count_documents({}),
                "agg": list(c.aggregate([{"$count": "n"}])), "distinct": sorted(c.distinct("v"))}
    return f


for _lvl in ("local", "available", "majority", "linearizable", "snapshot"):
    case(f"compat.readconcern.{_lvl}", [f'rc:level "{_lvl}"'], rs=True)(rc_probe(_lvl))


@case("compat.readconcern.afterClusterTime", ["rc:afterClusterTime"], rs=True)
def _act(client, db):
    seed(db)
    with client.start_session(causal_consistency=True) as s:
        db.t.insert_one({"_id": 99}, session=s)
        ts = s.operation_time
        r = db.command({"find": "t", "filter": {"_id": 99}, "readConcern": {"level": "majority", "afterClusterTime": ts}})
        r2 = db.command({"find": "t", "filter": {}, "readConcern": {"afterClusterTime": Timestamp(ts.time + 100000, 0), "level": "local"}, "maxTimeMS": 300})
        return {"hit": r["cursor"]["firstBatch"], "future": len(r2["cursor"]["firstBatch"])}


def wc_probe(wc, ack=True):
    def f(client, db):
        c = db.get_collection("t", write_concern=wc)
        r = c.insert_one({"_id": 1})
        out = {"ack": r.acknowledged, "n": db.t.count_documents({})}
        u = c.update_one({"_id": 1}, {"$set": {"a": 1}})
        out["upd"] = u.acknowledged and u.modified_count
        out["del"] = c.delete_one({"_id": 1}).acknowledged
        return out
    return f


case("compat.writeconcern.w_number", ["wc:w: <number>"], rs=True)(wc_probe(WriteConcern(w=1)))
case("compat.writeconcern.w_number_unsatisfiable", ["wc:w: <number>"], rs=True)(wc_probe(WriteConcern(w=3, wtimeout=500)))
case("compat.writeconcern.w_majority", ['wc:w: "majority"'], rs=True)(wc_probe(WriteConcern(w="majority")))
case("compat.writeconcern.w_0", ["wc:w: 0 (unacknowledged)"], rs=True)(wc_probe(WriteConcern(w=0)))
case("compat.writeconcern.j_true", ["wc:j"], rs=True)(wc_probe(WriteConcern(j=True)))
case("compat.writeconcern.wtimeout", ["wc:wtimeout"], rs=True)(wc_probe(WriteConcern(w=1, wtimeout=1000)))
case("compat.writeconcern.w_custom_tag_unknown", ["wc:w: <tag set name>"], rs=True, expect_error=True)(wc_probe(WriteConcern(w="nosuchtag", wtimeout=500)))


def rp_probe(pref):
    def f(client, db):
        host, port = client.address
        c = MongoClient(host, port, directConnection=True, read_preference=pref, serverSelectionTimeoutMS=8000)
        try:
            t = c[db.name].t
            t.insert_many([{"_id": i} for i in range(3)])
            return {"find": list(t.find().sort("_id", 1)), "agg": list(t.aggregate([{"$count": "n"}])), "count": t.count_documents({})}
        finally:
            c.close()
    return f


for _name, _pref in (("primary", Primary()), ("primaryPreferred", PrimaryPreferred()), ("secondary", Secondary()),
                     ("secondaryPreferred", SecondaryPreferred()), ("nearest", Nearest())):
    case(f"compat.readpreference.{_name}", [f"rp:{_name}"], rs=True)(rp_probe(_pref))
case("compat.readpreference.maxStalenessSeconds", ["rp:maxStalenessSeconds"], rs=True)(rp_probe(SecondaryPreferred(max_staleness=120)))
case("compat.readpreference.tag_sets", ["rp:tag sets"], rs=True)(rp_probe(SecondaryPreferred(tag_sets=[{"dc": "east"}, {}])))
case("compat.readpreference.hedge", ["rp:hedge"], rs=True)(rp_probe(Nearest(hedge={"enabled": True})))
