#!/usr/bin/env python3
"""Gross cost of a run: PUT/COPY/POST/LIST requests at the PUT price, GET and other requests at the GET price, plus
instance hours. Prices come from prices.json (next to this script's parent directory or in the same directory).

    cost.py <put_tier_requests> <get_tier_requests> <instance_seconds>   -> prints the cost in USD
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.join(HERE, "prices.json"), os.path.join(HERE, "..", "prices.json")):
    if os.path.exists(_p):
        PRICES = json.load(open(_p))
        break
else:
    raise SystemExit("prices.json not found")


def gross(put_tier, get_tier, instance_secs):
    return put_tier * PRICES["put_tier_usd_per_request"] + get_tier * PRICES["get_tier_usd_per_request"] + instance_secs / 3600.0 * PRICES["c6i_large_usd_per_hour"]


if __name__ == "__main__":
    print(f"{gross(float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])):.6f}")
