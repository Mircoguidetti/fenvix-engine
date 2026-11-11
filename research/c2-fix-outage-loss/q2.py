import sys, json
sys.path.insert(0, "/home/user/wt-rev/ctl-tests")
import lib
from lib import Server
lib.OUT = "/tmp/claude-0"
b, binary = sys.argv[1], sys.argv[2]
env = json.loads(sys.argv[3]) if len(sys.argv) > 3 else {"FENVIX_SEALED_RUNS": "off"}
s = Server(b, "q", idle=600, binary=binary, env=env).start()
try:
    c = s.client().lst.t
    ids = lambda cur: [d["_id"] for d in cur]
    print("natural range 31995..32005:", ids(c.find({"_id": {"$gte": 31995, "$lte": 32005}}, {"_id": 1}).hint([("$natural", 1)])))
    print("natural range 32000..32002:", ids(c.find({"_id": {"$gte": 32000, "$lte": 32002}}, {"_id": 1}).hint([("$natural", 1)])))
    print("index range 31995..32005:", ids(c.find({"_id": {"$gte": 31995, "$lte": 32005}}, {"_id": 1, "f": 1})))
    print("natural reverse:", ids(c.find({"_id": {"$gte": 31995, "$lte": 32005}}, {"_id": 1}).hint([("$natural", -1)])))
finally:
    s.term()
