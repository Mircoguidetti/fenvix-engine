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
# integration-4: 200,000 (was 100,000). With the clone-uploads fix a closed database is fully absorbed into sealed runs,
# so the cold find got faster (0.54-0.59 s against 1.10 s on the branch) and fell under the 3 T_pin this test needs.
N = 200_000


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
            got = [d["_id"] for d in c.d.w.find({}, {"_id": 1}, batch_size=20000)]
            rounds.append(time.time() - t0)
            assert sorted(got) == list(range(N)), f"{len(got)} ids of {N}"
        print(f"three finds of {N} ids on a reader: {', '.join(f'{r:.2f} s' for r in rounds)} (T_pin {T_PIN_MS} ms)")
        # the first find is the cold one; it must last several T_pin, or this test proves nothing
        assert rounds[0] * 1000 >= 3 * T_PIN_MS, f"the find lasted {rounds[0]:.2f} s, under 3 T_pin: make N larger"
        c.close()
    finally:
        s.term()
