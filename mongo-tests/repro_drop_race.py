#!/usr/bin/env python3
"""Reproduces/regresses the dropDatabase vs background compaction race: write enough closed sessions
to queue a compaction, drop the database immediately, then re-create it and read it back."""
import sys
from pymongo import MongoClient

uri = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27018"
rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 15
c = MongoClient(uri, serverSelectionTimeoutMS=15000)
bad = 0
for r in range(rounds):
    db = c[f"droprace{r % 3}"]
    try:
        for k in range(6):  # each cycle closes a writer: 6 L0 SSTs queue a compaction
            db.c.insert_one({"_id": k, "r": r})
            db.command({"tenantAdmin": 1, "close": True})
        c.drop_database(db.name)
        db.c.insert_many([{"_id": i} for i in range(3)])
        n = db.c.count_documents({})
        assert n == 3, f"expected 3 docs after re-create, found {n}"
    except Exception as e:  # noqa
        bad += 1
        print(f"round {r} ({db.name}): {str(e)[:160]}")
    c.drop_database(db.name)
print(f"{rounds - bad}/{rounds} rounds clean")
sys.exit(1 if bad else 0)
