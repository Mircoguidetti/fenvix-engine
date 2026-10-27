#!/usr/bin/env python3
"""Probe the engine (and, for non-destructive categories, real mongod 7.0) with one minimal request per inventory row to learn
whether the server recognises the name. 'unknown' means the server's own "unrecognised operator/stage/command" error for exactly
that name. Where real mongod ALSO reports unknown the probe is not valid for that row ('n/a': e.g. $box is only valid inside $geoWithin).
Usage: probe_engine.py [--ours mongodb://localhost:27018] [--real mongodb://localhost:27017] -> compat/probe_results.json"""
import argparse, csv, json, os, sys
from pymongo import MongoClient
HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--ours", default="mongodb://localhost:27018")
ap.add_argument("--real", default="mongodb://localhost:27017")
a = ap.parse_args()
inv = list(csv.DictReader(open(os.path.join(HERE, "..", "inventory_base.csv"))))
CL = {"ours": MongoClient(a.ours, serverSelectionTimeoutMS=15000, socketTimeoutMS=20000), "real": MongoClient(a.real, serverSelectionTimeoutMS=15000, socketTimeoutMS=20000)}
for c in CL.values():
    c.compat_probe.x.drop(); c.compat_probe.x.insert_one({"_id": 1, "a": 1, "b": [1, 2], "s": "x"})

TOP = {"$and", "$or", "$nor", "$expr", "$text", "$where", "$comment", "$jsonSchema"}


def req(rid, name):
    kind = rid.split(":", 1)[0]
    if kind == "qop":
        if name in TOP:
            return lambda d: list(d.x.find({name: 1}))
        return lambda d: list(d.x.find({"a": {name: 1}}))
    if kind == "expr":
        return lambda d: list(d.x.aggregate([{"$addFields": {"r": {name: []}}}]))
    if kind == "stage":
        if name in ("$changeStream", "$changeStreamSplitLargeEvent"):
            return None    # tailable: would wait for events forever; covered by the change-stream tests
        return lambda d: list(d.x.aggregate([{name: {}}]))
    if kind == "acc":
        return lambda d: list(d.x.aggregate([{"$group": {"_id": None, "r": {name: "$a"}}}]))
    if kind == "win":
        return lambda d: list(d.x.aggregate([{"$setWindowFields": {"sortBy": {"a": 1}, "output": {"r": {name: "$a", "window": {"documents": ["unbounded", "current"]}}}}}]))
    if kind == "upd":
        if name in ("$each", "$position", "$slice", "$sort", "$", "$[]", "$[<identifier>]"):
            return None
        return lambda d: d.x.update_one({}, {name: {"a": 1}}).modified_count
    if kind == "cmd":
        return lambda d: d.command({name: 1})
    return None


def classify(rid, name, e):
    kind = rid.split(":", 1)[0]
    msg = str(e)
    code = getattr(e, "code", None)
    if kind == "cmd":
        return "unknown" if code == 59 else "handled"
    pats = {"qop": [f"unknown operator: {name}", f"unknown top level operator: {name}"],
            "expr": [f"Unknown expression {name}", f"Unrecognized expression '{name}'"],
            "stage": [f"Unrecognized pipeline stage name: '{name}'"],
            "acc": [f"unknown group operator '{name}'"],
            "win": [f"Unrecognized window function, {name}"],
            "upd": [f"Unknown modifier: {name}"]}[kind]
    return "unknown" if any(p in msg for p in pats) else "handled"


out = {}
for r in inv:
    rid, name = r["id"], r["name"]
    f = req(rid, name)
    if f is None:
        continue
    res = {}
    for who in ("ours", "real"):
        if who == "real" and rid.startswith("cmd:"):
            continue        # never run arbitrary commands (shutdown, fsync, ...) on real mongod
        d = CL[who].compat_probe
        try:
            f(d); res[who] = "handled"; res[who + "_msg"] = "ok"
        except Exception as e:
            res[who] = classify(rid, name, e); res[who + "_msg"] = str(e)[:160]
    out[rid] = res
json.dump(out, open(os.path.join(HERE, "..", "probe_results.json"), "w"), indent=1, sort_keys=True)
n = {"unknown": 0, "handled": 0}
for v in out.values(): n[v["ours"]] += 1
print(len(out), n)
