import sys, time, signal, threading
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
from test_51r_review_c2_fix_clone_uploads import *
b = bucket("probe")
relay = Relay(0)
env = dict(ENV, FENVIX_SEALED_TPIN_MS="20000")
srv = Server(b, "r51-probe", idle=600, endpoint=relay.endpoint, extra=SMALL_L0, env=env).start()
expect = {}
try:
    for rnd, delay in enumerate([0.0, 0.05, 0.1, 0.2]):
        col = srv.client()["g"]["c"]
        write_round(col, rnd, 2000, expect)
        print("compactor", fenvix(srv.client())["compactor"], flush=True)
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=90000)
        c.admin.command("ping")
        t0 = time.time()
        done = []
        th = threading.Thread(target=lambda: (c["g"].command("tenantAdmin", close=True), done.append(time.time() - t0)), daemon=True)
        th.start()
        time.sleep(delay)
        relay.p.send_signal(signal.SIGSTOP)
        t1 = time.time()
        while time.time() - t1 < 25 and not done:
            time.sleep(0.1)
        relay.p.send_signal(signal.SIGCONT)
        th.join(60)
        log = srv.log_text()
        print(f"delay {delay}: close took {done}, freeze lasted {time.time()-t1:.1f}; absorb bound {log.count('were not absorbed before the close')}, flush bound {log.count('with the compactor running did not finish')}", flush=True)
        print("wrong", len(wrong(srv.client()["g"]["c"], expect)), flush=True)
finally:
    relay.p.send_signal(signal.SIGCONT)
    srv.term(); relay.stop()
