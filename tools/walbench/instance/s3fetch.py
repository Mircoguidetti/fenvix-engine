#!/usr/bin/env python3
"""Download one object from S3 with the instance-profile credentials (IMDSv2 + SigV4, standard library only), so the
instance needs no AWS CLI. usage: s3fetch.py <bucket> <region> <key> <dest>. Credentials stay in memory, never printed."""
import datetime, hashlib, hmac, json, sys, time, urllib.parse, urllib.request

def imds(path, tok):
    return urllib.request.urlopen(urllib.request.Request("http://169.254.169.254/latest/" + path, headers={"X-aws-ec2-metadata-token": tok}), timeout=5).read().decode()

def creds():
    for _ in range(120):  # the instance profile can take a few seconds to show up after boot
        try:
            tok = urllib.request.urlopen(urllib.request.Request("http://169.254.169.254/latest/api/token", method="PUT",
                                         headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"}), timeout=5).read().decode()
            role = imds("meta-data/iam/security-credentials/", tok).strip().splitlines()[0]
            return json.loads(imds("meta-data/iam/security-credentials/" + role, tok))
        except Exception:
            time.sleep(2)
    raise SystemExit("no instance profile credentials")

def sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

def get(bucket, region, key, dest):
    c = creds()
    host = f"{bucket}.s3.{region}.amazonaws.com"
    path = "/" + urllib.parse.quote(key, safe="/-_.~")
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    ph = hashlib.sha256(b"").hexdigest()
    hdrs = {"host": host, "x-amz-content-sha256": ph, "x-amz-date": amz, "x-amz-security-token": c["Token"]}
    signed = ";".join(sorted(hdrs))
    canon = "\n".join(["GET", path, "", "".join(f"{k}:{hdrs[k]}\n" for k in sorted(hdrs)), signed, ph])
    scope = f"{day}/{region}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canon.encode()).hexdigest()])
    k = sign(sign(sign(sign(("AWS4" + c["SecretAccessKey"]).encode(), day), region), "s3"), "aws4_request")
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    h = {k2: v for k2, v in hdrs.items() if k2 != "host"}
    h["Authorization"] = f"AWS4-HMAC-SHA256 Credential={c['AccessKeyId']}/{scope}, SignedHeaders={signed}, Signature={sig}"
    with urllib.request.urlopen(urllib.request.Request(f"https://{host}{path}", headers=h), timeout=120) as r, open(dest, "wb") as f:
        while True:
            b = r.read(1 << 20)
            if not b:
                break
            f.write(b)

if __name__ == "__main__":
    for attempt in range(10):
        try:
            get(*sys.argv[1:5])
            break
        except Exception as e:
            print(f"s3fetch {sys.argv[3]} attempt {attempt}: {type(e).__name__}", file=sys.stderr)
            time.sleep(3)
    else:
        sys.exit(1)
