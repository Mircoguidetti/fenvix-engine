"""K2c (K5 of the reviewer of K2b) · on the client machine, before the round of a size: open every database on the size's
cook with a light command (listCollections: a reader, no write, no document read) and keep them open while settle.py
watches the pantry, so whatever our server does when a database opens happens BEFORE the measures and not during them.
Every TOUCH_S seconds (20, under the server's --idle-secs 60) it lists the collections again; it stops after SECONDS or
when killed. Nothing measured here goes into the results of the round; the cache it fills is saved (serverStatus before
and after) so the "inizio" phase can be read with it.
Usage: open_all.py SECONDS OUT_JSON   (FENVIX_P1_URI from the environment; never printed)"""
import datetime as dt
import json
import os
import signal
import sys
import time

from pymongo import MongoClient

TOUCH_S = float(os.environ.get("OPEN_TOUCH_S", "20"))
SYSTEM = {"admin", "local", "config"}


def cache(c):
    try:
        w = c.admin.command("serverStatus").get("wiredTiger", {}).get("cache", {})
        return {"bytes_in_cache": w.get("bytes currently in the cache"), "pages_read": w.get("pages read into cache")}
    except Exception as e:  # noqa: BLE001
        return {"error": type(e).__name__}


def main():
    most, out = float(sys.argv[1]), sys.argv[2]
    rec = {"start": f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}", "touches": 0, "errors": []}
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    t0 = time.time()
    with MongoClient(os.environ["FENVIX_P1_URI"], appname="app-demo", serverSelectionTimeoutMS=20000) as c:
        rec["cache_before"] = cache(c)
        names = [d for d in c.list_database_names() if d not in SYSTEM]
        rec["databases"] = {}
        for d in names:
            t = time.time()
            try:
                rec["databases"][d] = {"collections": len(c[d].list_collection_names()),
                                       "first_open_ms": round((time.time() - t) * 1000, 1)}
            except Exception as e:  # noqa: BLE001
                rec["errors"].append(f"{d}: {type(e).__name__}")
        rec["cache_after_open"] = cache(c)
        print(f"open_all: {len(names)} databases opened ({', '.join(names)}), {len(rec['errors'])} errors", flush=True)
        json.dump(rec, open(out, "w"), indent=1)
        while not stop and time.time() - t0 < most:
            end = time.time() + TOUCH_S
            while not stop and time.time() < end:
                time.sleep(0.5)
            if stop:
                break
            for d in names:
                try:
                    c[d].list_collection_names()
                except Exception as e:  # noqa: BLE001
                    rec["errors"].append(f"{d}: {type(e).__name__}")
            rec["touches"] += 1
        rec["cache_at_end"] = cache(c)
    rec["seconds"] = round(time.time() - t0)
    json.dump(rec, open(out, "w"), indent=1)


if __name__ == "__main__":
    main()
