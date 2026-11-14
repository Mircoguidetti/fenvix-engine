# B1 strong point, SIZE 2 (10 GB logical, 4,732,850 documents), all three systems on one machine, 2026-11-14

Machine: 4 vCPU, 15 GiB RAM, cloud sandbox, Docker 29.9 (cgroup v1). Page cache dropped before every cold round (allowed here).
Engine: fenvix-engine b05d906 (fix-sim-holes-2), built per lines.py (`cargo build --release -p mongo-server`, `proto/quaderno: cargo build --release`), sha256 in binaries.txt.
Harness: bench-strong-point 5ac0023 + `--measure-sizes` (sizes 1 and 2 loaded, only size 2 settled and measured) + VACUUM (ANALYZE, PARALLEL 0)
(parallel vacuum failed on docker's 64 MB /dev/shm). Plan: `history.py plan` defaults (seed 11, sizes 5/10/15 GB), plan.json here, slot files by sha256.
Lines: fenvix-log (cook 512 MiB, log copies 2 GiB, `--customer-key messages=userId:ts`, no other flags), mongo-single (mongod 7.0, 512 MiB,
WT cache 0.25 GB), postgres-single (Postgres 16, 512 MiB, default shared_buffers). Driver: run.sh.

Cold reads, p50/p99 ms (ours round B = 20 ms per MinIO response; mongod and Postgres take no delay):

| read | ours B | ours A | mongod | Postgres | ours B / best p50 | S3 GETs p50 (bytes p50) |
|---|---|---|---|---|---|---|
| whole h50 | 32.07/40.12 | 12.15/20.51 | 9.97/20.04 | 3.51/6.53 | 9.14 | 7 (312,656) |
| whole h500 | 38.89/52.93 | 19.46/35.79 | 88.64/111.05 | 28.96/44.19 | 1.34 | 8 (769,969) |
| whole h5000 | 293.50/399.43 | 126.17/173.77 | 838.78/964.33 | 279.63/342.69 | 1.05 | 22 (7,656,843) |
| newest 10 | 41.16/66.96 | 20.87/44.94 | 3.51/7.83 | 1.26/2.39 | 32.7 | 13 (495,584) |

0 wrong reads on every line and round (every answer sha256-checked against the plan, so identical across systems); 300-customer
verification after each load clean; client CPU share at most 0.40 on every op (all VALID); no container OOM-killed. Full table: compare.out.
