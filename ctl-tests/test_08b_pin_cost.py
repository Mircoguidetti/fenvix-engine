"""Steady-state object-store cost of keeping a database awake, split by what causes it (counting proxy)."""
import json
import time

from lib import Server, make_bucket, fill, OUT
from proxy import CountingProxy


def test_pin_renewal_cost():
    bucket = make_bucket("pincost")
    proxy = CountingProxy()
    # round3: one lease per process is the default now; this test measures the per-database lease (an open database renews
    # its own lease), so it asks for that mode. The per-process mode is measured in test_20_process_lease.py.
    srv = Server(bucket, "pc", idle=600, endpoint=proxy.endpoint, extra=["--usage-flush-secs", "3600", "--pin-lease-ttl-secs", "12", "--lease-ttl-secs", "30", "--process-lease", "false"]).start()
    try:
        c = srv.client()
        fill(c, "p", "c", 100)
        srv.admin.put("/v1/databases/p/keep-awake")
        time.sleep(3)
        proxy.reset()
        window = 60
        time.sleep(window)
        snap = proxy.snapshot()
        mine = {k: v for k, v in snap["by_prefix"].items() if k.startswith("tenants/p") or k.startswith("control/pins")}
        print(json.dumps(mine, indent=1))
        pins = mine.get("control/pins/p", {})
        lease = mine.get("tenants/p", {})
        renewals = window / (12 / 3)
        # one conditional read-modify-write per renewal: a GET and a PUT, every ttl/3
        assert abs(pins.get("PUT", 0) - renewals) <= 2 and abs(pins.get("GET", 0) - renewals) <= 2, pins
        # the tenant's own lease (existing behaviour for any open tenant): a PUT every ttl/3 = 10 s
        assert abs(lease.get("PUT", 0) - window / 10) <= 2, lease
        json.dump({"window_s": window, "pin_ttl_s": 12, "tenant_lease_ttl_s": 30, "pin_object": pins, "tenant_prefix": lease, "other_control_prefixes": {k: v for k, v in mine.items() if k not in ("control/pins/p", "tenants/p")}}, open(f"{OUT}/pin_cost.json", "w"), indent=1)
    finally:
        srv.term()
        proxy.stop()
