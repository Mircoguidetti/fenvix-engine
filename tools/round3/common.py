"""Shared settings of the round3 measurements: MinIO on 127.0.0.1:9000 with its default test credentials (as
tools/minio-env.sh), the engine on loopback. Real S3 runs (tools/round3/s3/) override the AWS_* variables from the
GitHub Actions environment and never use these values."""
import json
import os
import re
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REAL_S3 = os.environ.get("ROUND3_REAL_S3") == "1"
if not REAL_S3:
    for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1",
                   "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
        os.environ[_k] = _v
    for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN"):
        os.environ.pop(_k, None)

ENGINE_BIN = os.environ.get("ROUND3_ENGINE_BIN", os.path.join(ROOT, "target", "release", "mongo-server"))
WORK = os.environ.get("ROUND3_WORK", "/tmp/round3-work")
ADMIN_TOKEN = "round3-test-token-0123456789"   # test value for a loopback-only admin port
os.makedirs(WORK, exist_ok=True)

# Prices of the brief (USD): PUT/COPY/POST/LIST per 1000, GET/HEAD per 1000, storage per GB-month. DELETE free.
PUT_PER_1000 = 0.005
GET_PER_1000 = 0.0004
GB_MONTH = 0.023
MONTH_S = 30 * 86400

PUT_CLASS = ("PutObject", "CopyObject", "ListObjectsV2", "ListObjectsV1", "ListMultipartUploads", "CreateMultipartUpload",
             "CompleteMultipartUpload", "UploadPart", "PostObject", "PutObjectPart", "NewMultipartUpload", "ListObjectVersions")
GET_CLASS = ("GetObject", "HeadObject", "HeadBucket", "GetBucketLocation")
FREE = ("DeleteObject", "DeleteMultipleObjects", "AbortMultipartUpload")

RE = re.compile(r'^minio_api_requests_total\{([^}]*)\}\s+([0-9.e+]+)', re.M)


def minio_counts():
    """Requests MinIO received, by S3 API name (what S3 would bill)."""
    with urllib.request.urlopen("http://127.0.0.1:9000/minio/metrics/v3/api/requests", timeout=10) as r:
        txt = r.read().decode()
    out = {}
    for labels, val in RE.findall(txt):
        m = re.search(r'name="([^"]+)"', labels)
        if m and 'type="s3"' in labels:
            out[m.group(1)] = out.get(m.group(1), 0) + float(val)
    return out


def diff(a, b):
    return {k: b.get(k, 0) - a.get(k, 0) for k in set(a) | set(b) if b.get(k, 0) - a.get(k, 0)}


def classify(d):
    """{api: n} -> (put_class, get_class, free, other)"""
    put = sum(v for k, v in d.items() if k in PUT_CLASS or k.startswith("List") or k.startswith("Put"))
    get = sum(v for k, v in d.items() if k in GET_CLASS or k.startswith("Get") or k.startswith("Head"))
    free = sum(v for k, v in d.items() if k in FREE or k.startswith("Delete") or k.startswith("Abort"))
    other = sum(d.values()) - put - get - free
    return put, get, free, other


def usd(put, get):
    return put / 1000 * PUT_PER_1000 + get / 1000 * GET_PER_1000


def per_db_month(put, get, secs, n_dbs):
    """Request cost per database per month, from counts over a window of `secs`."""
    return usd(put, get) / secs * MONTH_S / n_dbs


def proc_stat(pid):
    clk, page = os.sysconf("SC_CLK_TCK"), os.sysconf("SC_PAGE_SIZE")
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    with open(f"/proc/{pid}/statm") as f:
        rss = int(f.read().split()[1]) * page
    return {"cpu_s": (int(parts[11]) + int(parts[12])) / clk, "rss_mb": rss / 2**20, "fds": len(os.listdir(f"/proc/{pid}/fd"))}


def dump(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True)
    os.replace(path + ".tmp", path)


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]
