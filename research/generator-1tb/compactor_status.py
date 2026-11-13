"""The compactor's counters from our server's serverStatus (the "compactor" document under "fenvix", wherever it is
nested). Usage: compactor_status.py URI  (prints them as JSON)"""
import json
import sys

import pymongo


def find(d, key):
    if isinstance(d, dict):
        if key in d and isinstance(d[key], dict):
            return d[key]
        for v in d.values():
            r = find(v, key)
            if r is not None:
                return r
    return None


def compactor(client):
    return find(client.admin.command("serverStatus").get("fenvix", {}), "compactor")


if __name__ == "__main__":
    print(json.dumps(compactor(pymongo.MongoClient(sys.argv[1], directConnection=True))))
