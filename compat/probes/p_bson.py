"""BSON types, binData subtypes and the cross-type comparison order, checked against real mongod."""
import datetime, struct
import bson
from bson import Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from bson.raw_bson import RawBSONDocument
from probe_diff import case


def cstr(s):
    return s.encode() + b"\x00"


def bstr(s):
    b = s.encode()
    return struct.pack("<i", len(b) + 1) + b + b"\x00"


def raw(*elements):
    body = b"".join(elements) + b"\x00"
    return RawBSONDocument(struct.pack("<i", len(body) + 4) + body)


def el(t, name, payload=b""):
    return bytes([t]) + cstr(name) + payload


ID = lambda i: el(0x10, "_id", struct.pack("<i", i))
SCOPE = bson.encode({"x": 1})

# alias -> value to store (plain driver types) or a raw element for deprecated types the driver cannot write
VALUES = {
    "double": 1.5, "string": "s", "object": {"a": 1}, "array": [1, "a"], "objectId": ObjectId("507f1f77bcf86cd799439011"),
    "bool": True, "date": datetime.datetime(2020, 1, 2, 3, 4, 5, 678000), "null": None, "regex": Regex("ab+c", "i"),
    "javascript": Code("function(){return 1}"), "int": 7, "timestamp": Timestamp(1700000000, 5), "long": Int64(2 ** 40),
    "decimal": Decimal128("1.25"), "minKey": MinKey(), "maxKey": MaxKey(), "binData": Binary(b"abc", 0),
}
RAW_ELEMENTS = {   # deprecated types: written as raw BSON elements
    "undefined": el(0x06, "v"), "dbPointer": el(0x0C, "v", bstr("db.coll") + bytes(range(12))), "symbol": el(0x0E, "v", bstr("sym")),
    "javascriptWithScope": el(0x0F, "v", struct.pack("<i", 4 + len(bstr("return x")) + len(SCOPE)) + bstr("return x") + SCOPE),
}
NUM = {"double": 1, "string": 2, "object": 3, "array": 4, "binData": 5, "undefined": 6, "objectId": 7, "bool": 8, "date": 9, "null": 10, "regex": 11,
       "dbPointer": 12, "javascript": 13, "symbol": 14, "javascriptWithScope": 15, "int": 16, "timestamp": 17, "long": 18, "decimal": 19,
       "minKey": -1, "maxKey": 127}


def type_probe(alias):
    def f(client, db):
        c = db.t
        if alias in RAW_ELEMENTS:
            c.insert_one(raw(ID(1), RAW_ELEMENTS[alias]))
        else:
            c.insert_one({"_id": 1, "v": VALUES[alias]})
        out = {
            "doc": list(c.find({}, {"_id": 0})),
            "type": list(c.aggregate([{"$project": {"_id": 0, "t": {"$type": "$v"}}}])),
            "by_alias": c.count_documents({"v": {"$type": alias}}),
            "by_number": c.count_documents({"v": {"$type": NUM[alias]}}),
            "not_other": c.count_documents({"v": {"$type": "string" if alias != "string" else "int"}}),
        }
        return out
    return f


for _alias in NUM:
    case(f"compat.bson.type_{_alias}", [f"bson:{_alias}"])(type_probe(_alias))


def binsub_probe(sub):
    def f(client, db):
        c = db.t
        data = bytes(range(16))
        c.insert_one({"_id": 1, "v": Binary(data, sub)})
        return {"doc": [(d["v"].subtype, bytes(d["v"])) for d in c.find({})] if False else list(c.find({}, {"_id": 0})),
                "type": list(c.aggregate([{"$project": {"_id": 0, "t": {"$type": "$v"}}}])),
                "eq": c.count_documents({"v": Binary(data, sub)}), "ne_other": c.count_documents({"v": Binary(data, 0 if sub else 128)})}
    return f


for _s in (0, 1, 2, 3, 4, 5, 6, 7, 128):
    case(f"compat.bson.binsubtype_{_s}", [f"binsub:{_s}"])(binsub_probe(_s))


ORDER_DOCS = [
    (1, MaxKey()), (2, ObjectId("507f1f77bcf86cd799439011")), (3, True), (4, datetime.datetime(2020, 1, 1)), (5, Timestamp(5, 1)),
    (6, Regex("a", "")), (7, Code("x")), (8, MinKey()), (9, None), (10, 5), (11, Int64(3)), (12, 4.5), (13, Decimal128("2")), (14, "str"),
    (15, {"a": 1}), (16, [1, 2]), (17, Binary(b"ab", 0)), (18, [])]


def sort_all_types(client, db):
    c = db.t
    c.insert_many([{"_id": i, "v": v} for i, v in ORDER_DOCS])
    c.insert_one(raw(ID(100), RAW_ELEMENTS["symbol"]))
    c.insert_one(raw(ID(101), RAW_ELEMENTS["javascriptWithScope"]))
    c.insert_one({"_id": 102})  # missing field
    return {"asc": [d["_id"] for d in c.find({}, {"_id": 1}).sort([("v", 1), ("_id", 1)])]}


def sort_all_types_desc(client, db):
    sort_all_types(client, db)
    return {"desc": [d["_id"] for d in db.t.find({}, {"_id": 1}).sort([("v", -1), ("_id", 1)])]}


case("compat.bson.sort_all_types_asc", [f"bsonord:{i}" for i in range(1, 16)] + ["bsoncmp:Non-existent Fields"])(sort_all_types)
case("compat.bson.sort_all_types_desc_with_empty_array", ["bsoncmp:Arrays"])(sort_all_types_desc)


@case("compat.bson.compare_numeric", ["bsoncmp:Numeric Types"])
def _num(client, db):
    c = db.t
    vals = [1, Int64(1), 1.0, Decimal128("1"), 1.5, Decimal128("1.5"), float("nan"), Decimal128("NaN"), float("inf"), -float("inf"), Int64(2 ** 53 + 1), float(2 ** 53), -0.0, 0, Decimal128("0.1"), 0.1]
    c.insert_many([{"_id": i, "v": v} for i, v in enumerate(vals)])
    return {"sort": [d["_id"] for d in c.find().sort([("v", 1), ("_id", 1)])],
            "eq1": [d["_id"] for d in c.find({"v": 1})], "gt1": [d["_id"] for d in c.find({"v": {"$gt": 1}})],
            "eqnan": [d["_id"] for d in c.find({"v": float("nan")})], "lt_inf": [d["_id"] for d in c.find({"v": {"$lt": float("inf")}})],
            "cmp": list(c.aggregate([{"$project": {"c": {"$cmp": ["$v", 1]}}}, {"$sort": {"_id": 1}}]))}


@case("compat.bson.compare_strings", ["bsoncmp:Strings"])
def _str(client, db):
    c = db.t
    vals = ["a", "B", "b", "A", "é", "e", "Z", "z", "", " ", "aa", "a\u0000b", "\U0001F600", "ab", "Ａ"]
    c.insert_many([{"_id": i, "v": v} for i, v in enumerate(vals)])
    return {"sort": [d["_id"] for d in c.find().sort("v", 1)], "gt": [d["_id"] for d in c.find({"v": {"$gt": "b"}}).sort("_id", 1)],
            "cmp": list(c.aggregate([{"$project": {"c": {"$strcasecmp": ["$v", "A"]}}}, {"$sort": {"_id": 1}}]))}


@case("compat.bson.compare_arrays", ["bsoncmp:Arrays"])
def _arr(client, db):
    c = db.t
    vals = [[1, 2], [3], [], [[1]], [0, 9], [None], [1, "a"], [5, 1], 4, [2, 2]]
    c.insert_many([{"_id": i, "v": v} for i, v in enumerate(vals)])
    return {"asc": [d["_id"] for d in c.find().sort([("v", 1), ("_id", 1)])], "desc": [d["_id"] for d in c.find().sort([("v", -1), ("_id", 1)])],
            "gt2": [d["_id"] for d in c.find({"v": {"$gt": 2}})], "lt2": [d["_id"] for d in c.find({"v": {"$lt": 2}})],
            "gte_arr": [d["_id"] for d in c.find({"v": {"$gte": [1, 2]}})]}


@case("compat.bson.compare_objects", ["bsoncmp:Objects"])
def _obj(client, db):
    c = db.t
    vals = [{"a": 1}, {"a": 1, "b": 2}, {"b": 1}, {"a": "x"}, {}, {"a": None}, {"a": 1, "b": {"c": 1}}, {"a": 1, "b": {"c": 0}}, {"B": 1}]
    c.insert_many([{"_id": i, "v": v} for i, v in enumerate(vals)])
    return {"asc": [d["_id"] for d in c.find().sort([("v", 1), ("_id", 1)])], "eq": [d["_id"] for d in c.find({"v": {"a": 1}})],
            "gt": [d["_id"] for d in c.find({"v": {"$gt": {"a": 1}}})]}


@case("compat.bson.compare_dates_timestamps", ["bsoncmp:Dates and Timestamps"])
def _dt(client, db):
    c = db.t
    vals = [datetime.datetime(1960, 1, 1), datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 1, 0, 0, 0, 1000), Timestamp(0, 1), Timestamp(5, 1), Timestamp(5, 2),
            Timestamp(1, 0)]
    c.insert_many([{"_id": i, "v": v} for i, v in enumerate(vals)])
    return {"asc": [d["_id"] for d in c.find().sort([("v", 1), ("_id", 1)])], "gt": [d["_id"] for d in c.find({"v": {"$gt": datetime.datetime(2000, 1, 1)}})],
            "tsgt": [d["_id"] for d in c.find({"v": {"$gt": Timestamp(1, 0)}})]}


@case("compat.bson.compare_bindata", ["bsoncmp:BinData"])
def _bin(client, db):
    c = db.t
    vals = [Binary(b"\x01\x02", 0), Binary(b"\x01", 5), Binary(b"\x00\x00\x00", 0), Binary(b"\x02", 0), Binary(b"\x01", 0), Binary(b"\xff\x00", 0), Binary(b"\x01", 128),
            Binary(b"", 0)]
    c.insert_many([{"_id": i, "v": v} for i, v in enumerate(vals)])
    return {"asc": [d["_id"] for d in c.find().sort([("v", 1), ("_id", 1)])], "gt": [d["_id"] for d in c.find({"v": {"$gt": Binary(b"\x01", 0)}})]}
