"""Shared settings of the density measurements (tools/density). Only local services: MinIO on 127.0.0.1:9000 with its
default test credentials (the same as tools/minio-env.sh), the engine on loopback. No AWS, no real secrets."""
import os

for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1",
               "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN"):
    os.environ.pop(_k, None)

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENGINE_BIN = os.environ.get("DENSITY_ENGINE_BIN", "/home/user/eng-1c9fee7/target/release/mongo-server")
WORK = os.environ.get("DENSITY_WORK", "/home/user/density-work")      # temporary: certificates, logs, raw latencies
RESULTS = os.path.join(ROOT, "results-density")
BUCKET = os.environ.get("DENSITY_BUCKET", "density")
PORT, ADMIN_PORT = 27100, 27101
ADMIN_TOKEN = "density-test-token-0123456789"   # test value for a loopback-only admin port
ROOT_USER, ROOT_PW = "root", "density-root-pw"  # test values, local engine only
CERT = os.path.join(WORK, "tls", "server.pem")   # self-signed test certificate (key + cert)
CA = os.path.join(WORK, "tls", "ca.pem")
ENGINE_CPUS = os.environ.get("DENSITY_ENGINE_CPUS", "0,1")   # the engine is pinned to 2 CPUs (a t3.medium has 2 vCPUs)
OTHER_CPUS = os.environ.get("DENSITY_OTHER_CPUS", "2,3")     # MinIO, load clients, sampler

def db_name(i):
    return f"c{i:04d}"

def user_pw(i):
    return f"pw-{i:04d}-density"   # test password of a local test user

os.makedirs(WORK, exist_ok=True)
