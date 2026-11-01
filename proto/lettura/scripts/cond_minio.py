#!/usr/bin/env python3
"""Comportamento di MinIO sulle scritture condizionali (If-Match / If-None-Match), codici HTTP grezzi.

Si cerca il bug noto in cui MinIO rispondeva 304 invece di 412 a un PUT condizionale fallito: per lo scarico
"da chiunque" conta solo che un PUT con If-Match sbagliato NON scriva e risponda con un codice d'errore.
Uso: venv/bin/python cond_minio.py [endpoint]  (credenziali minioadmin, bucket cond-test)
"""
import sys, threading, json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ep = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9000"
s3 = boto3.client("s3", endpoint_url=ep, aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
                  region_name="us-east-1", config=Config(retries={"max_attempts": 0}))
B = "cond-test"
try:
    s3.create_bucket(Bucket=B)
except ClientError:
    pass

def call(f, **kw):
    try:
        r = f(Bucket=B, **kw)
        return r["ResponseMetadata"]["HTTPStatusCode"], r.get("ETag")
    except ClientError as e:
        return e.response["ResponseMetadata"]["HTTPStatusCode"], e.response["Error"].get("Code")

def body(k):
    return s3.get_object(Bucket=B, Key=k)["Body"].read()

res = {}
k = "m1"
try:
    s3.delete_object(Bucket=B, Key=k)
except ClientError:
    pass
res["put_if_none_match_nuovo"] = call(s3.put_object, Key=k, Body=b"v1", IfNoneMatch="*")
res["put_if_none_match_esistente"] = call(s3.put_object, Key=k, Body=b"XX", IfNoneMatch="*")
res["contenuto_dopo_if_none_match_fallito"] = body(k).decode()
et = s3.head_object(Bucket=B, Key=k)["ETag"]
res["put_if_match_giusto"] = call(s3.put_object, Key=k, Body=b"v2", IfMatch=et)
res["put_if_match_sbagliato_vecchio_etag"] = call(s3.put_object, Key=k, Body=b"YY", IfMatch=et)
res["put_if_match_sbagliato_inventato"] = call(s3.put_object, Key=k, Body=b"ZZ", IfMatch='"0123456789abcdef0123456789abcdef"')
res["contenuto_dopo_if_match_falliti"] = body(k).decode()
res["put_if_match_chiave_assente"] = call(s3.put_object, Key="non-esiste-" + k, Body=b"q", IfMatch=et)
et2 = s3.head_object(Bucket=B, Key=k)["ETag"]
res["put_if_none_match_etag_uguale"] = call(s3.put_object, Key=k, Body=b"WW", IfNoneMatch=et2)
res["contenuto_dopo_put_if_none_match_etag"] = body(k).decode()
et2 = s3.head_object(Bucket=B, Key=k)["ETag"]
res["get_if_none_match_stesso_etag"] = call(s3.get_object, Key=k, IfNoneMatch=et2)
res["get_if_match_sbagliato"] = call(s3.get_object, Key=k, IfMatch=et)

# gara: 32 PUT con lo stesso If-Match in parallelo, ne deve vincere uno solo
et3 = et2
codes = []
lock = threading.Lock()
def racer(i):
    c = call(s3.put_object, Key=k, Body=("gara-%d" % i).encode(), IfMatch=et3)
    with lock:
        codes.append(c[0])
ts = [threading.Thread(target=racer, args=(i,)) for i in range(32)]
[t.start() for t in ts]
[t.join() for t in ts]
res["gara_32_if_match_codici"] = {str(c): codes.count(c) for c in sorted(set(codes))}
res["gara_vincitore_contenuto"] = body(k).decode()

# gara su creazione: 32 PUT If-None-Match:* su una chiave nuova
k2 = "m2"
try:
    s3.delete_object(Bucket=B, Key=k2)
except ClientError:
    pass
codes = []
def racer2(i):
    c = call(s3.put_object, Key=k2, Body=("crea-%d" % i).encode(), IfNoneMatch="*")
    with lock:
        codes.append(c[0])
ts = [threading.Thread(target=racer2, args=(i,)) for i in range(32)]
[t.start() for t in ts]
[t.join() for t in ts]
res["gara_32_if_none_match_codici"] = {str(c): codes.count(c) for c in sorted(set(codes))}

print(json.dumps(res, indent=1))
ok = (res["put_if_none_match_esistente"][0] == 412 and res["put_if_match_sbagliato_vecchio_etag"][0] == 412
      and res["put_if_match_sbagliato_inventato"][0] == 412 and res["contenuto_dopo_if_match_falliti"] == "v2"
      and res["gara_32_if_match_codici"].get("200") == 1 and res["gara_32_if_none_match_codici"].get("200") == 1)
print("ESITO:", "conforme a S3 (412, un solo vincitore)" if ok else "NON conforme: vedi sopra")
