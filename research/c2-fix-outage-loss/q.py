import sys, os, json
sys.path.insert(0, "/home/user/wt-rev/ctl-tests")
import lib
from lib import Server
lib.OUT = "/tmp/claude-0"
b, binary, mode = sys.argv[1], sys.argv[2], sys.argv[3]
env = {"FENVIX_SEALED_RUNS": "off"} if mode == "off" else {"FENVIX_SEALED_TPIN_MS": "30000"}
s = Server(b, "q", idle=600, binary=binary, env=env).start()
try:
    c = s.client().lst.t
    for i in json.loads(sys.argv[4]):
        print(i, "find_one:", (lambda d: d and {k: (v[:12] if isinstance(v, str) else v) for k, v in d.items()})(c.find_one({"_id": i})), "hint_natural:", list(c.find({"_id": i}, {"_id": 1}).hint([("$natural", 1)])))
    print("count", c.estimated_document_count(), c.count_documents({}))
finally:
    s.term()
