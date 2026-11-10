"""The cells of the competitor measurement and where each one's data lives.

A cell is one set of threads run through the savers. For each cell:
  kind        scripted (owner), real (reviewer), edge (reviewer)
  mongo_db    the Mongo database the MongoDBSaver wrote in DEFAULT mode (the one we dump)
  ...         the keys needed to find the measured and the published numbers
"""
import os

C2 = os.environ.get("C2", "/home/user/c2data")
RUN = os.path.join(C2, "run", "research", "checkpoint-delta")  # working copy the runs write into
SRC_OWNER = os.path.join(C2, "src", "owner", "research", "checkpoint-delta")  # published, untouched
SRC_REV = os.path.join(C2, "src", "reviewer", "research", "checkpoint-delta", "review")  # published, untouched
DUMPS = os.path.join(C2, "dumps")
MONGO = "mongodb://127.0.0.1:27017/"
PG = "postgresql://postgres:pg@127.0.0.1:5432/"

SCRIPTED = [(10, 400), (40, 150), (100, 40), (300, 8)]
REAL = [("tau_gpt_4o_airline", 200), ("tau_sonnet_35_new_retail", 300), ("swe4", 80), ("swe35", 80)]
EDGE = [(40, 3), (100, 8)]
POLICIES = ("base", "summarize", "edit", "remove")


def cells():
    out = []
    for n, t in SCRIPTED:
        out.append({"cell": f"scripted_{n}x{t}", "kind": "scripted", "n": n, "threads": t,
                    "mongo_db": f"ck_default_{n}", "mongo_db_delta": f"ck_delta_{n}"})
    for ds, t in REAL:
        out.append({"cell": f"real_{ds}", "kind": "real", "ds": ds, "threads": t,
                    "mongo_db": f"rv_{ds}_default", "mongo_db_delta": f"rv_{ds}_delta"})
    for n, t in EDGE:
        for p in POLICIES:
            out.append({"cell": f"edge_{p}_{n}x{t}", "kind": "edge", "policy": p, "n": n, "threads": t,
                        "mongo_db": f"edge_{p}_default_{n}", "mongo_db_delta": f"edge_{p}_delta_{n}"})
    return out
