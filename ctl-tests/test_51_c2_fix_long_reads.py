"""c2-fix-long-reads: a long read on a database that woke as a reader is never refused because of T_pin.

integration-3, open item 1: with sealed runs on, the takeover program's read-back (one find of about 523,000 ids, about
60 s) was refused once the reader's state was T_pin old ("... was last read 30.0 s ago, more than T_pin"): the reader
renewed its state only under the database's exclusive lock, between commands, and one find runs all its steps under the
shared lock. Here T_pin is 200 ms (production 30 s) and the server runs with the product defaults otherwise (sealed runs,
the compactor). A find of every id of a database that woke as a reader must last many times T_pin and return every id.

On the code before the fix (MONGO_SERVER_BIN=<integration-3 build>) this test fails: the find is refused.
"""
import time

from lib import Server, make_bucket

T_PIN_MS = 200
# 300,000: at 100,000 the find took 0.44 s on GitHub runners, under 3 T_pin (CI on 3cb6b38); integration-4 needed
# 200,000 for the same reason (clone-uploads made the cold find faster).
N = 300_000


def test_long_find_on_a_reader_is_not_refused():
    s = Server(make_bucket("c2long"), "c2long", idle=3, env={"FENVIX_SEALED_TPIN_MS": str(T_PIN_MS)}).start()
    try:
        c = s.client()
        for b in range(N // 5000):
            c.d.w.insert_many([{"_id": b * 5000 + i, "p": "x" * 100} for i in range(5000)])
        c.close()
        # the writer closes when idle (3 s); the next read wakes the database as a reader
        time.sleep(8)
        c = s.client()
        rounds = []
        for _ in range(3):
            t0 = time.time()
            # sorted by _id: one command reads every id (a plain find is now read page by page, one short command per
            # getMore, fix-long-commands), so this stays the long single command the test is about
            got = [d["_id"] for d in c.d.w.find({}, {"_id": 1}, batch_size=20000).sort("_id", 1)]
            rounds.append(time.time() - t0)
            assert got == list(range(N)), f"{len(got)} ids of {N}"
        print(f"three finds of {N} ids on a reader: {', '.join(f'{r:.2f} s' for r in rounds)} (T_pin {T_PIN_MS} ms)")
        # the first find is the cold one; it must last several T_pin, or this test proves nothing
        assert rounds[0] * 1000 >= 3 * T_PIN_MS, f"the find lasted {rounds[0]:.2f} s, under 3 T_pin: make N larger"
        c.close()
    finally:
        s.term()
