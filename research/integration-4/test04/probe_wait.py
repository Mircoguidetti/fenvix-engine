"""test_04's bounded-memory case with a wait between the close and the export (integration-4, CI 292f526). If the RSS
growth goes away with the wait, it comes from work after the close (compaction pass, absorption, GC), not from the
export's streaming. Usage (from ctl-tests, sealed env set): python probe_wait.py WAIT_S [BINARY]"""
import os, sys, tempfile, threading, time, shutil
sys.path.insert(0, os.getcwd())
from lib import Server, make_bucket, TOKEN
import export_client as xc

wait_s = float(sys.argv[1])
if len(sys.argv) > 2:
    os.environ["MONGO_SERVER_BIN"] = sys.argv[2]
srv = Server(make_bucket("exprobe"), "exprobe", idle=300, binary=sys.argv[2] if len(sys.argv) > 2 else None).start()
try:
    c = srv.client()
    big = [{"_id": i, "p": "q" * 380, "n": i} for i in range(100000)]
    for i in range(0, 100000, 5000):
        c["mem"]["c"].insert_many(big[i:i + 5000])
    c["mem"].command("tenantAdmin", close=True)
    time.sleep(wait_s)

    def rss():
        for line in open(f"/proc/{srv.proc.pid}/status"):
            if line.startswith("VmRSS"):
                return int(line.split()[1]) // 1024
    st, r = srv.admin.post("/v1/databases/mem/exports")
    dest = tempfile.mkdtemp(prefix="dump-")
    base = rss(); peak = [base]; stop = threading.Event()
    def sampler():
        while not stop.is_set():
            peak[0] = max(peak[0], rss()); time.sleep(0.02)
    t = threading.Thread(target=sampler); t.start()
    xc.download(f"http://127.0.0.1:{srv.admin_port}", TOKEN, r["id"], dest)
    stop.set(); t.join()
    print(f"wait {wait_s:.0f} s: RSS {base} -> {peak[0]} MB, growth {peak[0] - base} MB")
    shutil.rmtree(dest)
finally:
    srv.term()
