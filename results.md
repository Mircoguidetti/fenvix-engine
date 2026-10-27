# Many idle slatedb tenants on object storage — benchmark results

**Claim under test:** thousands of small, mostly idle databases stored on object storage cost almost nothing, and an idle one can wake up fast.

**Verdict (measured locally against MinIO, priced at S3 list prices):**

- **Cost: confirmed, with one condition.** A *closed* tenant makes **0 requests** while idle (measured over 60 s, both runs), so its only cost is storage: **~$9.10/month for 1,000,000 tenants** (~$4.60 once obsolete WAL files are garbage-collected), against **$8,000,000/month** for Atlas Flex at $8/tenant. A wake costs about **$0.000054** in requests (**$0.000068** including the close).
- **The condition:** "idle" must mean **closed**. An *open* slatedb `Db` with no traffic still polls object storage (~3.2 GETs/s, mostly manifest polling), which is **~$3.30/month per tenant**, or ~$3.3M/month if 1,000,000 tenants were left open. The product needs to close idle tenants.
- **Wake-up speed: fast against local MinIO.** Cold open + first get: **p50 ≈ 35 ms, p99 ≈ 48–122 ms**. The p99 varies between runs because 100 samples give a noisy p99. Each wake makes ~44 sequential requests, so on real S3 (typically 10–30 ms per small GET/PUT, higher at the tail) expect **hundreds of milliseconds**. The local number is a lower bound, not a prediction.
- **Warm reads** on an open tenant: p50 ≈ 0.07 ms and p99 ≈ 2.5 ms, served from slatedb's in-memory block cache.

## Headline cost table (N = 10,000 run)

| Item | Basis | Cost |
|---|---|---|
| Storage, 1,000,000 idle (closed) tenants | 424,607 B/tenant (measured) → 395.45 GB × $0.023 | **$9.10 / month** |
| Same, after slatedb GC removes the obsolete WAL SST | 214,955 B/tenant → 200.19 GB × $0.023 | $4.60 / month |
| Idle requests, closed tenant | 0 requests in 60 s | $0.00 / month |
| Wake one tenant (cold open + 1 get) | 7.96 PUT/LIST-tier + 35.90 GET-tier requests | **$0.0000542 per wake** |
| Close after a wake | 2.17 PUT/LIST-tier + 7.03 GET-tier requests | $0.0000137 per close |
| Initial load of one tenant (200 × 1 KB docs) | 17.11 PUT/LIST-tier + 50.32 GET-tier requests | $0.000106 per tenant |
| Tenant kept **open** with no traffic | 190 GETs / 60 s, extrapolated to 30 days | $3.28 / month per tenant |
| **MongoDB Atlas Flex, 1,000,000 tenants** | $8 × 1,000,000 | **$8,000,000 / month** |
| Break-even | ($8M − storage) / (wake + close) | ≈ 1.18 × 10¹¹ wakes/month ≈ 118,000 wakes per tenant per month |

Prices used (AWS S3 Standard, us-east-1): storage $0.023 per GB-month (GB = 2³⁰ bytes, as AWS bills); PUT/COPY/POST/LIST $0.005 per 1,000; GET (and HEAD) $0.0004 per 1,000; DELETE free.

## Latency summary

| | N = 1,000 | N = 10,000 |
|---|---|---|
| Cold open + get, p50 | 34.26 ms | 35.59 ms |
| Cold open + get, p99 | 122.23 ms | 47.91 ms |
| Warm get, p50 | 0.065 ms | 0.071 ms |
| Warm get, p99 | 2.497 ms | 2.506 ms |

> **Latency caveat.** MinIO runs on localhost, so each request takes well under a millisecond on the network, while real S3 takes tens of milliseconds per request with a long tail. **Treat every latency number here as a lower bound.** The **cost numbers are still valid** because they come only from bytes stored and requests counted, and those do not depend on how fast the backend is. The request counts are the same against S3 because the client code is identical.

## Findings that matter for the product

1. **Idle closed tenants make no requests.** A closed slatedb `Db` has no background process, so nothing polls storage. Measured: 0 requests in 60 s with 1,000 and with 10,000 tenants at rest.
2. **Open tenants do make requests.** Default `Settings` poll the manifest every 1 s and run an in-process compactor and GC. Measured ~3.2 GET/s for one open tenant with no traffic. Tenants must be closed (or run with much longer poll intervals) when idle.
3. **About half of the stored bytes are obsolete WAL.** After `flush` + `close`, each tenant holds a ~205 KiB WAL SST *and* a ~208 KiB compacted SST holding the same data. slatedb's GC deletes the WAL after `min_age` (5 min), but it runs **only inside an open `Db`** (every 10 min). A tenant that is closed right after writing keeps the WAL indefinitely unless a separate GC job runs. The table shows both figures, and both are negligible compared with Atlas.
4. **A wake is not read-only.** Opening a `Db` as the writer makes **5 PUTs**, and closing makes 1 more PUT. These are writer-fencing and state writes: new manifest versions, compactions-state files, and an empty WAL SST. Every wake therefore also adds a few small objects (roughly 1 KB per wake), which stay until GC removes them. If a tenant only needs to be read, a read-only reader (`DbReader`) would avoid these writes. That variant was not measured here.
5. **Wake cost depends mostly on PUT-tier requests.** ~8 PUT/LIST-tier requests at $0.005/1k cost $0.00004, compared with $0.000014 for ~36 GETs. At ~$0.00007 per wake+close, 1,000,000 tenants waking 100 times each per month cost about $6,800/month in requests. That is still about 1,000× cheaper than Atlas Flex.

## Method

- **Stack:** Rust (rustc 1.97.0), `slatedb = "=0.17.0"`, `object_store = "=0.14.2"` (feature `aws`), MinIO in Docker. No cloud account and no paid API was used. All traffic went to `http://localhost:9000`.
- **Versions:** slatedb **0.17.0** was the latest stable on crates.io (checked 2026-10-25), and its dependency is `object_store ^0.14.2`. object_store **0.14.2** was also the latest stable, so both are pinned exactly. `cargo tree -i object_store` confirms there is a single object_store version in the build. Other deps (resolved in `Cargo.lock`): tokio, futures, async-trait, rand 0.8, clap 4, serde/serde_json, anyhow.
- **API confirmation:** docs.rs is blocked by this session's network policy, so I checked the APIs against the **published crate sources and README** of slatedb 0.17.0 and object_store 0.14.2 (the same doc comments that docs.rs renders). Calls used: `Db::open(path, Arc<dyn ObjectStore>)`, `Db::put` (documented as *not* awaiting durability), `Db::flush`, `Db::get`, `Db::close`, all with default `Settings`.
- **Counting layer** (`src/counting.rs`) implements `ObjectStore` and forwards to `AmazonS3`:
  - PUT-type: `put_opts` (covers `put`), `put_multipart_opts` + each `put_part` + `complete`, `copy_opts`, `rename_opts` (counted as copy + delete).
  - GET-type: `get_opts`, split into get / get_range / head (`head`, `get`, and `get_range` all route through it in 0.14), `list` / `list_with_offset` (+1 per extra 1,000-key page), and `list_with_delimiter`. `get_ranges` is deliberately not overridden: the default implementation coalesces ranges and issues each HTTP request through `get_range`, so each request is counted exactly once.
  - For pricing, LIST is billed at the PUT/COPY/POST/LIST tier even though it is grouped under "GET-type" in the raw counts. DELETE is counted but free.
  - Not visible to the counting layer: retries performed inside object_store's own HTTP client. This has no effect against a healthy local MinIO.
- **Phases** (single process, sequential):
  1. Delete everything under `tenants/` (so each run starts clean).
  2. **Load:** for each tenant `tenants/{id}`: open, put 200 JSON docs of exactly 1,024 bytes (`doc:{i}`), flush, close. 32 tenants run concurrently.
  3. **Storage:** list `tenants/` on the raw (uncounted) store and aggregate bytes and objects per tenant and per slatedb directory.
  4. **Cold read:** 100 distinct random tenants, none opened since load. Measures the time of `Db::open` + one random `get`, and records the requests of open+get and of the following close separately.
  5. **Idle, closed:** sleep 60 s with every tenant closed and record all requests.
  6. **Warm read:** open one reserved tenant (open time not included), then do 1,000 random gets and time each one.
  7. **Idle, open (extra contrast measurement):** keep that tenant open with no traffic for 60 s and record its requests.
- Percentiles use the nearest-rank method.

## Deviations from the requested setup (and why)

- **MinIO image:** `quay.io/minio/minio` is blocked by this session's egress policy (HTTP 403), and `minio/minio` is no longer published on Docker Hub. I used **`bitnamilegacy/minio:latest`**, which ships the upstream MinIO server binary (`minio version DEVELOPMENT.2025-05-24T17-08-30Z`, digest `sha256:451fe6858cb770cc9d0e77ba811ce287420f781c7c1b806a386f6896471a349c`). It runs with the same arguments, ports, and credentials. I overrode the entrypoint to the `minio` binary and ran the container as root (`--user 0`), because the image's default uid cannot write `/data`.
- **Bucket creation:** used `bitnamilegacy/minio-client` (`mc`) on a shared Docker network (`benchnet`), and verified the bucket with `mc ls`.
- **Docker daemon** was installed but not running. I started `dockerd` in the container.
- **docs.rs** is unreachable here, so I read the API docs from the crate sources instead (see Method).

## Reproduce

```bash
# Docker daemon (if not already running)
sudo dockerd > dockerd.log 2>&1 &

# MinIO + bucket
docker network create benchnet
docker run -d --name minio --user 0 --network benchnet -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  --entrypoint /opt/bitnami/minio/bin/minio bitnamilegacy/minio:latest \
  server /data --console-address ":9001"
#   (where quay.io is reachable, the original image works too:
#    docker run -d --name minio --network benchnet -p 9000:9000 -p 9001:9001 \
#      -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
#      quay.io/minio/minio server /data --console-address ":9001")
docker run --rm --network benchnet --entrypoint sh bitnamilegacy/minio-client:latest -c \
  'mc alias set local http://minio:9000 minioadmin minioadmin && mc mb --ignore-existing local/bench && mc ls local'

# Benchmark (writes runs/n{N}.md and runs/n{N}.json)
cargo build --release
cargo run --release -- --tenants 1000     # default N = 1000
cargo run --release -- --tenants 10000
# options: --concurrency 32 (load phase), --idle-secs 60, --out-dir runs
```

Raw per-run data: `runs/n1000.json`, `runs/n10000.json`.

## Full run reports (generated by the benchmark binary)

### Run: 1000 tenants

| Measurement | Value |
|---|---|
| Load time (1000 tenants × 200 docs, concurrency 32) | 20.7 s (20.7 ms/tenant wall) |
| Load requests total | PUT-type 11148 · GET-type 55790 · delete 0 |
| Load requests per tenant | PUT-type 11.15 (put 11.15, multipart 0.00, copy 0.00) · GET-type 55.79 (get 40.96, get_range 0.00, head 9.00, list 5.83) · delete 0.00 |
| Storage total | 11148 objects, 424596535 bytes (404.93 MiB) |
| Storage per tenant (avg) | 424597 bytes, 11.15 objects |
| Storage per tenant by directory (avg) | compacted/ 213547 B · compactions/ 110 B · manifest/ 1298 B · wal/ 209642 B |
| Cold open+get latency (100 samples) | p50 34.26 ms · p99 122.23 ms (min 29.56, mean 37.91, max 174.45) |
| Cold open+get requests per wake | PUT-type 5.00 (put 5.00, multipart 0.00, copy 0.00) · GET-type 38.72 (get 24.42, get_range 2.33, head 9.00, list 2.97) · delete 0.00 |
| Close-after-wake requests per wake | PUT-type 1.00 (put 1.00, multipart 0.00, copy 0.00) · GET-type 8.32 (get 7.16, get_range 0.00, head 0.00, list 1.16) · delete 0.00 |
| Warm get latency (1000 samples, one open tenant) | p50 0.065 ms · p99 2.497 ms (min 0.056, mean 0.180, max 3.851) |
| Requests during 1000 warm gets | PUT-type 0 · GET-type 104 (get 31, get_range 68, head 0, list 5) |
| Idle **closed** tenants, 60 s | **0 requests** (PUT-type 0, GET-type 0, delete 0) |
| Idle **open** tenant (no traffic), 60 s | 194 requests (PUT-type 0, GET-type 194: get 194, get_range 0, head 0, list 0; delete 0) |

#### Cost model (S3 Standard us-east-1 list prices)

| Item | Basis | Cost |
|---|---|---|
| Storage, 1,000,000 idle tenants / month | 424597 B/tenant × 1e6 = 395.44 GB × $0.023 | **$9.10/month** |
| Storage, 1,000,000 tenants, obsolete WAL removed by GC | 214955 B/tenant × 1e6 = 200.19 GB × $0.023 | $4.60/month |
| Idle request cost, closed tenant | 0 requests in 60 s | $0.00000000/month |
| Wake one tenant (cold open + 1 get) | 7.97 PUT/LIST-tier + 35.75 GET-tier requests | **$0.00005415 per wake** |
| Close after wake (flush on close) | 2.16 PUT/LIST-tier + 7.16 GET-tier requests | $0.00001366 per close |
| Initial load of one tenant (200 docs) | 16.97 PUT/LIST-tier + 49.96 GET-tier requests | $0.00010486 per tenant |
| One tenant kept **open** but idle | extrapolated from 194 requests / 60 s | $3.35/month per tenant ($3,352,320.00/month for 1e6) |
| MongoDB Atlas Flex, 1,000,000 tenants | $8 × 1e6 | **$8,000,000.00/month** |
| Ratio Atlas / object-storage idle | | 879601× |
| Wakes per month that equal Atlas cost | ($8e6 − storage) / (wake + close) | 1.180e11 wakes/month (117970 per tenant) |

### Run: 10000 tenants

| Measurement | Value |
|---|---|
| Load time (10000 tenants × 200 docs, concurrency 32) | 209.0 s (20.9 ms/tenant wall) |
| Load requests total | PUT-type 112229 · GET-type 562106 · delete 0 |
| Load requests per tenant | PUT-type 11.22 (put 11.22, multipart 0.00, copy 0.00) · GET-type 56.21 (get 41.32, get_range 0.00, head 9.00, list 5.89) · delete 0.00 |
| Storage total | 112229 objects, 4246073943 bytes (4049.37 MiB) |
| Storage per tenant (avg) | 424607 bytes, 11.22 objects |
| Storage per tenant by directory (avg) | compacted/ 213547 B · compactions/ 110 B · manifest/ 1298 B · wal/ 209652 B |
| Cold open+get latency (100 samples) | p50 35.59 ms · p99 47.91 ms (min 28.59, mean 36.09, max 49.37) |
| Cold open+get requests per wake | PUT-type 5.00 (put 5.00, multipart 0.00, copy 0.00) · GET-type 38.86 (get 24.51, get_range 2.39, head 9.00, list 2.96) · delete 0.00 |
| Close-after-wake requests per wake | PUT-type 1.00 (put 1.00, multipart 0.00, copy 0.00) · GET-type 8.20 (get 7.03, get_range 0.00, head 0.00, list 1.17) · delete 0.00 |
| Warm get latency (1000 samples, one open tenant) | p50 0.071 ms · p99 2.506 ms (min 0.032, mean 0.195, max 9.703) |
| Requests during 1000 warm gets | PUT-type 0 · GET-type 102 (get 29, get_range 68, head 0, list 5) |
| Idle **closed** tenants, 60 s | **0 requests** (PUT-type 0, GET-type 0, delete 0) |
| Idle **open** tenant (no traffic), 60 s | 190 requests (PUT-type 0, GET-type 190: get 190, get_range 0, head 0, list 0; delete 0) |

#### Cost model (S3 Standard us-east-1 list prices)

| Item | Basis | Cost |
|---|---|---|
| Storage, 1,000,000 idle tenants / month | 424607 B/tenant × 1e6 = 395.45 GB × $0.023 | **$9.10/month** |
| Storage, 1,000,000 tenants, obsolete WAL removed by GC | 214955 B/tenant × 1e6 = 200.19 GB × $0.023 | $4.60/month |
| Idle request cost, closed tenant | 0 requests in 60 s | $0.00000000/month |
| Wake one tenant (cold open + 1 get) | 7.96 PUT/LIST-tier + 35.90 GET-tier requests | **$0.00005416 per wake** |
| Close after wake (flush on close) | 2.17 PUT/LIST-tier + 7.03 GET-tier requests | $0.00001366 per close |
| Initial load of one tenant (200 docs) | 17.11 PUT/LIST-tier + 50.32 GET-tier requests | $0.00010569 per tenant |
| One tenant kept **open** but idle | extrapolated from 190 requests / 60 s | $3.28/month per tenant ($3,283,200.00/month for 1e6) |
| MongoDB Atlas Flex, 1,000,000 tenants | $8 × 1e6 | **$8,000,000.00/month** |
| Ratio Atlas / object-storage idle | | 879578× |
| Wakes per month that equal Atlas cost | ($8e6 − storage) / (wake + close) | 1.180e11 wakes/month (117956 per tenant) |
