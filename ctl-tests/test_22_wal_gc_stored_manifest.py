"""The hot WAL cleanup of an open writer must use the STORED manifest, and only while this process owns the database.

slatedb 0.17 moves the writer's in-memory replay boundary (`replay_after_wal_id`) when a memtable flush is applied, BEFORE the
manifest PUT that makes it durable. If the WAL pass trusts the in-memory manifest, it can delete WAL objects that the stored
manifest still needs; a crash before the PUT lands then loses acknowledged writes.

Fault injection (`MONGO_TEST_FAULTS_FILE`, test-only): the manifest PUTs of one database hang (the point between the flush
being applied in memory and the manifest write). Meanwhile the hot WAL pass runs every second. Then kill -9, restart without
the fault, and every acknowledged write must be readable.

Without the fix (the pass used `db.manifest()`), the WAL below the in-memory boundary is deleted and the restart cannot
replay it: acknowledged documents are missing or the database fails to open.
"""
import os
import tempfile
import time

from lib import Server, make_bucket

DOC = "x" * 4000


def test_crash_while_manifest_put_hangs_keeps_every_acknowledged_write():
    bucket = make_bucket("walgc")
    faults = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    faults.close()
    # memtable 1 MiB: a few hundred 4 KB documents make slatedb flush it to L0 (and write a manifest)
    extra = ["--memtable-max-mb", "1", "--wal-gc-secs", "1"]
    srv = Server(bucket, "walgc", idle=600, extra=extra, holder="walgc-h", env={"MONGO_TEST_FAULTS_FILE": faults.name}).start()
    acked = []
    try:
        c = srv.client(retryWrites=False)
        coll = c["w"]["c"]
        for i in range(20):
            coll.insert_one({"_id": i, "p": DOC})
            acked.append(i)
        # from now on the manifest PUTs of this database hang: flushes are applied in memory but never stored
        with open(faults.name, "w") as f:
            f.write("hang_put tenants/w/manifest/\n")
        # one durable write at a time (each is its own WAL object), well past one memtable
        for i in range(20, 900):
            coll.insert_one({"_id": i, "p": DOC})
            acked.append(i)
        deadline = time.time() + 60
        while "FAULT hang_put tenants/w/manifest/" not in srv.log_text() and time.time() < deadline:
            coll.insert_one({"_id": len(acked), "p": DOC})
            acked.append(len(acked))
        assert "FAULT hang_put tenants/w/manifest/" in srv.log_text(), "no memtable flush reached the manifest write"
        time.sleep(4)   # several hot WAL passes while the manifest PUT hangs
        st = c.admin.command("tenantStats")
        print("walGc:", st.get("walGc"))
    finally:
        srv.kill9()
    os.unlink(faults.name)
    srv2 = Server(bucket, "walgc2", idle=600, extra=[], holder="walgc-h").start()   # same holder: takes its database back at once
    try:
        c2 = srv2.client()
        got = {d["_id"] for d in c2["w"]["c"].find({}, {"_id": 1})}
        missing = [i for i in acked if i not in got]
        print(f"acknowledged {len(acked)}, missing after restart {len(missing)}")
        assert not missing, f"{len(missing)} acknowledged writes lost, first {missing[:5]}"
    finally:
        srv2.term()
