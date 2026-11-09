#!/usr/bin/env python3
"""Compaction 2 step 6: publish a version of our manifest through the server (the stub of step 4's reseal trigger).

  sealed.py publish --uri URI --db chat [--reseal all|none] [--block-kb 256]
      retries while E1's announcement waits (rule 26), prints the build report (JSON) and the staged publish's part.
  sealed.py counter --uri URI [--clear]
      what the counter of our runs holds (fenvixCounter).
"""
import argparse
import json
import time

from pymongo import MongoClient


def publish(admin, db, reseal="all", block_kb=None, tries=120):
    cmd = {"fenvixSealedPublish": db, "reseal": reseal}
    if block_kb:
        cmd["sealedBlockKb"] = int(block_kb)
    for _ in range(tries):
        r = admin.command(cmd)
        if r.get("published"):
            rep = json.loads(r["report"])
            rep["millis"] = r["millis"]
            return rep
        time.sleep(0.5)
    raise SystemExit(f"no publish of {db} after {tries} tries")


def counter(admin, clear=False):
    return admin.command({"fenvixCounter": 1, "clear": bool(clear)})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["publish", "counter"])
    ap.add_argument("--uri", required=True)
    ap.add_argument("--db", default="chat")
    ap.add_argument("--reseal", default="all")
    ap.add_argument("--block-kb", type=int)
    ap.add_argument("--clear", action="store_true")
    a = ap.parse_args()
    admin = MongoClient(a.uri).admin
    if a.cmd == "publish":
        print(json.dumps(publish(admin, a.db, a.reseal, a.block_kb)), flush=True)
    else:
        c = counter(admin, a.clear)
        c.pop("perCustomer", None)
        print(json.dumps({k: v for k, v in c.items()}), flush=True)


if __name__ == "__main__":
    main()
