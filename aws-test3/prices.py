#!/usr/bin/env python3
"""S3 Standard prices of eu-central-1 read from AWS's official price list (public file, no credentials), with its
publication date. Writes <out>/prices.json. Fails (exit 1) if the file cannot be read or a price is missing: the test
must not run on prices from memory.   usage: prices.py <out_dir>"""
import json, os, sys, time, urllib.request
URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-central-1/index.json"
d = json.load(urllib.request.urlopen(URL, timeout=120))
want = {"EUC1-Requests-Tier1": "put_tier_usd_per_request", "EUC1-Requests-Tier2": "get_tier_usd_per_request"}
out = {"source": URL, "publication_date": d.get("publicationDate"), "read_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "descriptions": {}}
for sku, p in d["products"].items():
    a = p["attributes"]
    if a.get("location") != "EU (Frankfurt)":
        continue
    ut = a.get("usagetype", "")
    for t in d["terms"]["OnDemand"].get(sku, {}).values():
        for dim in t["priceDimensions"].values():
            usd = float(dim["pricePerUnit"]["USD"])
            if ut in want:
                out[want[ut]] = usd
                out["descriptions"][want[ut]] = dim["description"]
            if ut == "EUC1-TimedStorage-ByteHrs" and dim["beginRange"] == "0":
                out["storage_usd_per_gb_month"] = usd
                out["descriptions"]["storage_usd_per_gb_month"] = dim["description"]
missing = [k for k in ("put_tier_usd_per_request", "get_tier_usd_per_request", "storage_usd_per_gb_month") if k not in out]
if missing:
    sys.exit(f"prices missing in the official file: {missing}")
os.makedirs(sys.argv[1], exist_ok=True)
json.dump(out, open(os.path.join(sys.argv[1], "prices.json"), "w"), indent=1)
print(json.dumps({k: out[k] for k in ("publication_date", "put_tier_usd_per_request", "get_tier_usd_per_request", "storage_usd_per_gb_month")}))
