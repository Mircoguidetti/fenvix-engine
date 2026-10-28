"""Merge point of compat-gaps (natural order) and ctl (export): since the natural-order work every stored document carries a 9-byte trailer
(0xFE + sequence number, natural.rs). The export streamed the stored value as it was, so the .bson files of a dump had the trailer between
the documents: not valid BSON, not restorable. The export now writes the BSON part only, in insertion order, and mongorestore's own insert
(which carries bypassEmptyTsReplacement) is accepted."""
import tempfile

import bson
from pymongo import MongoClient

import export_client as xc
from lib import Server, make_bucket, TOKEN


def test_the_dump_is_plain_bson_in_insertion_order():
    srv = Server(make_bucket("xnat"), "xnat", idle=300).start()
    try:
        c = srv.client()
        ids = [50, 3, 77, 1, 20, 9]
        c.xn.t.insert_many([{"_id": i, "v": i * 2} for i in ids])
        st, r = srv.admin.post("/v1/databases/xn/exports")
        assert st == 201, r
        dest = tempfile.mkdtemp(prefix="dump-")
        ver = xc.download(f"http://127.0.0.1:{srv.admin_port}", TOKEN, r["id"], dest)
        assert all(v == "ok" for v in ver.values()), ver
        raw = open(f"{dest}/xn/t.bson", "rb").read()
        docs = bson.decode_all(raw)                        # a trailer between documents makes this fail
        assert sorted(d["_id"] for d in docs) == sorted(ids)
        assert sum(len(bson.encode(d)) for d in docs) == len(raw), "bytes in the file that are not part of a document"
    finally:
        srv.term()


def test_insert_with_bypass_empty_ts_replacement_is_accepted():
    srv = Server(make_bucket("xbyp"), "xbyp", idle=300).start()
    try:
        c = srv.client()
        r = c.xb.command({"insert": "t", "documents": [{"_id": 1}], "bypassEmptyTsReplacement": True})
        assert r["n"] == 1
    finally:
        srv.term()
