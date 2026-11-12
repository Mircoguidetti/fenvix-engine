"""fix-erase-500 (2026-11-12): a read racing the end of an erase must not make the erase answer 500 erase_incomplete.

The erase's second proof listing (after the intent record goes) ran after the database's entry lock was released, so a
read of the name in that gap reopened it and its `tenants/<db>/_lease` was listed as "objects remain" (2 of 13 sealed U1
runs in the review). The failpoint `erase.before_proof_two` pauses the erase right before that listing; three readers of the name
run during the pause until the erase answers. The erase must answer 200, no read may find a document, and nothing may be left under the name.
"""
import threading
import time

from lib import Server, make_bucket, list_prefix, wait_until


def test_a_read_during_the_last_proof_does_not_fail_the_erase():
    b = make_bucket("eproof")
    s = Server(b, "eproof", idle=300, env={"CTL_FAILPOINT": "erase.before_proof_two", "CTL_FAILPOINT_PAUSE_MS": "2000"}).start()
    try:
        c = s.client()
        c.gone.t.insert_many([{"_id": i, "v": "x" * 100} for i in range(200)])
        res = {}
        th = threading.Thread(target=lambda: res.update(r=s.admin.delete("/v1/databases/gone")))
        th.start()
        wait_until(lambda: "FAILPOINT erase.before_proof_two reached" in s.log_text(), 30, what="failpoint")
        stop = threading.Event()
        errors = []

        def reader():
            cr = s.client()
            while not stop.is_set():
                try:
                    assert cr.gone.t.find_one({"_id": 5}) is None
                except AssertionError as e:
                    errors.append(repr(e))
                except Exception:
                    pass  # a read refused while the erase holds the name is fine

        rs = [threading.Thread(target=reader) for _ in range(3)]
        for t in rs:
            t.start()
        th.join(timeout=60)
        stop.set()
        for t in rs:
            t.join(timeout=60)
        assert not errors, errors[:3]
        st, body = res["r"]
        assert st == 200, (st, body)
        time.sleep(1)
        assert [k for k, _ in list_prefix(b, "tenants/gone/") if not k.endswith("/_lease")] == []
        wait_until(lambda: list_prefix(b, "tenants/gone/") == [], 15, what="no lease left under the erased name")
    finally:
        s.term()
