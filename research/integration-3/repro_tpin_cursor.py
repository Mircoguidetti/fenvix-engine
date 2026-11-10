# integration-3: with sealed runs on, a cursor on a database that woke as a reader (after an idle close) fails once it
# lives longer than T_pin. Here T_pin is 2 s (FENVIX_SEALED_TPIN_MS), production 30 s; the takeover read-back of 549,162
# ids hit the same refusal with 30 s.
import os, sys, time
from lib import Server, make_bucket
srv = Server(make_bucket("tpin"), "tpin", idle=3, extra=os.environ.get("EXTRA", "").split()).start()
c = srv.client()
for b in range(20):
    c.d.w.insert_many([{"_id": b * 1000 + i, "p": "x" * 50} for i in range(1000)])
time.sleep(float(os.environ.get("IDLE_WAIT", "8")))  # the writer closes (idle 3 s); the next read wakes a reader
n = 0
try:
    cur = c.d.w.find({}, {"_id": 1}, batch_size=1000)
    for d in cur:
        n += 1
        if n % 1000 == 0:
            time.sleep(0.5)  # a slow consumer: the cursor lives about 10 s
    print("cursor read", n, "of 20000: OK")
except Exception as e:
    print("cursor read", n, "of 20000: ERROR", str(e)[:200])
srv.term()
