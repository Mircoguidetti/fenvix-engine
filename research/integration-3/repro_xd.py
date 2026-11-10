# integration-3: after kill -9 with sealed runs on, the database refuses to open (X_d > SlateDB's last seq).
# Are acknowledged writes missing? Reopen the same bucket with sealed runs off and count.
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else ".")
from lib import Server, make_bucket
from proxy import CountingProxy
sealed = dict(FENVIX_SEALED_RUNS="2", FENVIX_SEALED_TPIN_MS="1000", FENVIX_SEALED_E1_WAIT_MS="0", FENVIX_SEALED_TEST_SHORT_E1_WAIT="1", FENVIX_SEALED_COMPACTOR=os.environ.get("C", "0"))
if os.environ.get("SEALED", "1") == "1":
    os.environ.update(sealed)
else:
    os.environ["FENVIX_SEALED_RUNS"] = "off"
b = make_bucket("xdrepro")
proxy = CountingProxy() if os.environ.get("PROXY") == "1" else None
extra = ["--usage-flush-secs", "1"] if os.environ.get("UF") == "1" else []
kw = {"endpoint": proxy.endpoint} if proxy else ({"endpoint": os.environ["RELAY"]} if os.environ.get("RELAY") else {})
srv = Server(b, "xdrepro", idle=int(os.environ.get("IDLE","3")), extra=extra, **kw).start()
c = srv.client()
for i in range(20):
    c["u6"].command({"insert": "c", "documents": [{"_id": i}]})
time.sleep(2.5)
srv.restart()
c = srv.client()
for i in range(20, 35):
    c["u6"].command({"insert": "c", "documents": [{"_id": i}]})
time.sleep(2.0)
srv.kill9(); srv.proc = None
srv.start()
for attempt in range(3):
    try:
        srv.client()["u6"].command({"insert": "c", "documents": [{"_id": 1000 + attempt}]})
        print(f"sealed on after kill -9: write {attempt} OK")
        break
    except Exception as e:
        print(f"sealed on after kill -9: write {attempt} ERROR", str(e)[:160])
        time.sleep(5)
try:
    print("sealed on after kill -9: count", srv.client()["u6"]["c"].count_documents({}))
except Exception as e:
    print("count ERROR", str(e)[:120])
srv.term()
os.environ["FENVIX_SEALED_RUNS"] = "off"
srv2 = Server(b, "xdrepro2", idle=600).start()
ids = sorted(d["_id"] for d in srv2.client()["u6"]["c"].find({}, {"_id": 1}))
print("sealed off on the same bucket: count", len(ids), "missing", sorted(set(range(35)) - set(ids)))
srv2.term()
