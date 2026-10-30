# results-walbench: where to put the durable write log (branch `walbench`)

Run `41017093239`, eu-west-1. Client and EBS volume in AZ id `euw1-az1` (the AZ of the Express directory bucket); quorum nodes in AZ ids euw1-az1, euw1-az2, euw1-az3. Instances c6i.large, AMI `ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20261023`. Raw JSON: `results-walbench/raw/`. Method, variants and the pass criteria (fixed before the run): `docs/walbench.md`.

vCPU quota check: {"quota_vcpus": 8.0, "used_vcpus": 2, "free_vcpus": 6.0, "max_instances": 3}. **Quorum node 0 ran as a process on the client instance** (only 3 instances allowed); nodes 1 and 2 on their own instances.

## Against the pass criteria

Criteria: zero acknowledged writes lost; p99 ack < 15 ms in every closed-loop cell; variable cost < 1.5 USD per million writes at concurrency 8 and 64 and in the shared-batch case (200 databases). The cost at concurrency 1 is reported but does not fail a configuration.

| backend | variant | loss | worst p99 (ms) | p99 | worst USD / M at conc 8, 64 | cost | USD / M at conc 1 (reported only) | configuration |
|---|---|---|---|---|---|---|---|---|
| S3 Standard | perwrite | 0 lost (complete) | 157.99 | FAIL | 5.001 | FAIL | 5.001 | fail |
| S3 Standard | w1 | 0 lost (complete) | 49.85 | FAIL | 0.627 | pass | 5.001 | fail |
| S3 Standard | w20 | 0 lost (complete) | 77.06 | FAIL | 0.627 | pass | 5.001 | fail |
| S3 Standard | w5 | 0 lost (complete) | 72.47 | FAIL | 0.627 | pass | 5.001 | fail |
| S3 Express One Zone | perwrite | 0 lost (complete) | 386.55 | FAIL | 1.137 | pass | 1.137 | fail |
| S3 Express One Zone | w1 | 0 lost (complete) | 19.97 | FAIL | 0.148 | pass | 1.137 | fail |
| S3 Express One Zone | w20 | 0 lost (complete) | 35.08 | FAIL | 0.148 | pass | 1.137 | fail |
| S3 Express One Zone | w5 | 0 lost (complete) | 31.82 | FAIL | 0.148 | pass | 1.137 | fail |
| DynamoDB on-demand | perwrite | 0 lost (complete) | 338.33 | FAIL | 1.430 | pass | 1.419 | fail |
| DynamoDB on-demand | w1 | 0 lost (complete) | 82.29 | FAIL | 0.803 | pass | 1.419 | fail |
| DynamoDB on-demand | w20 | 0 lost (complete) | 28.93 | FAIL | 0.803 | pass | 1.419 | fail |
| DynamoDB on-demand | w5 | 0 lost (complete) | 85.12 | FAIL | 0.803 | pass | 1.419 | fail |
| EBS gp3 + fdatasync | w0 | 0 lost (complete) | 1.47 | pass | 0.000 | pass | 0.000 | **PASS** |
| 3-AZ quorum (2 of 3) | w0 | 1 harness failures | 3.40 | pass | 0.039 | pass | 0.042 | fail |
| S3 Standard | fleet perdb perwrite (200 dbs × 1.0/s) | 0 lost (complete) | 93.66 | FAIL | 5.001 | FAIL | – | reference (not judged) |
| S3 Standard | fleet shared w1 (200 dbs × 1.0/s) | 0 lost (complete) | 70.74 | FAIL | 3.819 | FAIL | – | fail |
| S3 Standard | fleet shared w20 (200 dbs × 1.0/s) | 0 lost (complete) | 63.31 | FAIL | 0.970 | pass | – | fail |
| S3 Standard | fleet shared w5 (200 dbs × 1.0/s) | 0 lost (complete) | 65.96 | FAIL | 2.362 | FAIL | – | fail |
| S3 Express One Zone | fleet perdb perwrite (200 dbs × 1.0/s) | 0 lost (complete) | 14.16 | pass | 1.137 | pass | – | reference (not judged) |
| S3 Express One Zone | fleet shared w1 (200 dbs × 1.0/s) | 0 lost (complete) | 12.18 | pass | 0.878 | pass | – | **PASS** |
| S3 Express One Zone | fleet shared w20 (200 dbs × 1.0/s) | 0 lost (complete) | 32.45 | FAIL | 0.227 | pass | – | fail |
| S3 Express One Zone | fleet shared w5 (200 dbs × 1.0/s) | 0 lost (complete) | 17.19 | FAIL | 0.549 | pass | – | fail |
| DynamoDB on-demand | fleet perdb perwrite (200 dbs × 1.0/s) | 0 lost (complete) | 5.91 | pass | 1.419 | pass | – | reference (not judged) |
| DynamoDB on-demand | fleet shared w1 (200 dbs × 1.0/s) | 0 lost (complete) | 7.71 | pass | 1.253 | pass | – | **PASS** |
| DynamoDB on-demand | fleet shared w20 (200 dbs × 1.0/s) | 0 lost (complete) | 27.78 | FAIL | 0.851 | pass | – | fail |
| DynamoDB on-demand | fleet shared w5 (200 dbs × 1.0/s) | 0 lost (complete) | 11.54 | pass | 1.049 | pass | – | **PASS** |

Closed-loop configurations: latency judged on all 6 cells (200 B / 1 KB × conc 1, 8, 64), cost on the 4 cells at conc 8 and 64. Shared-batch configurations (200 databases, 1 KB) are judged on their cell; one log per database is shown as the reference. Loss is judged per backend (all its crash cycles, plus the quorum scenario / EBS reattach cycles).

Backends with at least one configuration meeting all three criteria: S3 Express One Zone, DynamoDB on-demand, EBS gp3 + fdatasync.
No other ranking is made.

## Ack latency (closed loop, ms)

Each cell: 100 warm-up writes, then up to 1500 measured writes (or 90 s). `conc` writers on one log, each waits for its ack. Acks are released in log order.

| backend | variant | size | conc | p50 | p95 | p99 | max | writes/s | avg batch | requests / write | variable USD / M |
|---|---|---|---|---|---|---|---|---|---|---|---|
| S3 Standard | perwrite | 200 B | 1 | 23.00 | 28.36 | 46.58 | 157.64 | 42 | 1.0 | 1.000 | 5.000 |
| S3 Standard | perwrite | 200 B | 8 | 25.07 | 35.87 | 59.60 | 72.81 | 302 | 1.0 | 1.000 | 5.000 |
| S3 Standard | perwrite | 200 B | 64 | 36.71 | 76.34 | 157.99 | 158.04 | 1332 | 1.0 | 1.000 | 5.000 |
| S3 Standard | perwrite | 1024 B | 1 | 23.49 | 30.77 | 45.97 | 84.81 | 41 | 1.0 | 1.000 | 5.001 |
| S3 Standard | perwrite | 1024 B | 8 | 26.09 | 37.78 | 57.10 | 71.93 | 291 | 1.0 | 1.000 | 5.001 |
| S3 Standard | perwrite | 1024 B | 64 | 36.66 | 80.97 | 83.13 | 106.98 | 1478 | 1.0 | 1.000 | 5.001 |
| S3 Standard | w1 | 200 B | 1 | 25.57 | 31.09 | 47.74 | 85.10 | 38 | 1.0 | 1.000 | 5.000 |
| S3 Standard | w1 | 200 B | 8 | 24.60 | 30.00 | 49.85 | 61.44 | 311 | 8.0 | 0.125 | 0.627 |
| S3 Standard | w1 | 200 B | 64 | 29.70 | 33.32 | 33.50 | 33.52 | 2119 | 62.5 | 0.016 | 0.080 |
| S3 Standard | w1 | 1024 B | 1 | 25.73 | 33.68 | 46.97 | 82.85 | 37 | 1.0 | 1.000 | 5.001 |
| S3 Standard | w1 | 1024 B | 8 | 25.68 | 32.85 | 46.39 | 80.49 | 299 | 8.0 | 0.125 | 0.627 |
| S3 Standard | w1 | 1024 B | 64 | 28.05 | 31.95 | 35.97 | 35.98 | 2221 | 62.5 | 0.016 | 0.081 |
| S3 Standard | w20 | 200 B | 1 | 44.55 | 53.72 | 69.33 | 110.67 | 22 | 1.0 | 1.000 | 5.000 |
| S3 Standard | w20 | 200 B | 8 | 48.24 | 56.13 | 63.02 | 90.56 | 163 | 8.0 | 0.125 | 0.627 |
| S3 Standard | w20 | 200 B | 64 | 44.80 | 48.18 | 77.06 | 77.07 | 1356 | 62.5 | 0.016 | 0.080 |
| S3 Standard | w20 | 1024 B | 1 | 44.54 | 53.14 | 74.05 | 115.27 | 22 | 1.0 | 1.000 | 5.001 |
| S3 Standard | w20 | 1024 B | 8 | 44.44 | 49.09 | 76.14 | 89.68 | 176 | 8.0 | 0.125 | 0.627 |
| S3 Standard | w20 | 1024 B | 64 | 46.98 | 50.98 | 51.37 | 51.38 | 1328 | 62.5 | 0.016 | 0.081 |
| S3 Standard | w5 | 200 B | 1 | 30.52 | 39.12 | 56.79 | 90.24 | 32 | 1.0 | 1.000 | 5.000 |
| S3 Standard | w5 | 200 B | 8 | 29.92 | 37.66 | 54.40 | 61.36 | 260 | 8.0 | 0.125 | 0.627 |
| S3 Standard | w5 | 200 B | 64 | 28.66 | 32.78 | 33.87 | 33.88 | 2182 | 62.5 | 0.016 | 0.080 |
| S3 Standard | w5 | 1024 B | 1 | 29.60 | 35.67 | 52.22 | 196.00 | 33 | 1.0 | 1.000 | 5.001 |
| S3 Standard | w5 | 1024 B | 8 | 31.28 | 41.38 | 72.47 | 76.14 | 243 | 8.0 | 0.125 | 0.627 |
| S3 Standard | w5 | 1024 B | 64 | 35.95 | 46.01 | 48.16 | 48.16 | 1712 | 62.5 | 0.016 | 0.081 |
| S3 Express One Zone | perwrite | 200 B | 1 | 7.32 | 10.05 | 11.20 | 22.66 | 130 | 1.0 | 1.000 | 1.132 |
| S3 Express One Zone | perwrite | 200 B | 8 | 9.37 | 33.48 | 41.09 | 50.90 | 572 | 1.0 | 1.000 | 1.132 |
| S3 Express One Zone | perwrite | 200 B | 64 | 62.55 | 267.18 | 386.55 | 466.31 | 701 | 1.0 | 1.000 | 1.132 |
| S3 Express One Zone | perwrite | 1024 B | 1 | 7.42 | 10.35 | 11.51 | 13.39 | 128 | 1.0 | 1.000 | 1.137 |
| S3 Express One Zone | perwrite | 1024 B | 8 | 9.44 | 32.88 | 38.08 | 43.10 | 579 | 1.0 | 1.000 | 1.137 |
| S3 Express One Zone | perwrite | 1024 B | 64 | 61.52 | 268.07 | 385.01 | 453.01 | 676 | 1.0 | 1.000 | 1.137 |
| S3 Express One Zone | w1 | 200 B | 1 | 9.52 | 12.49 | 13.77 | 18.88 | 101 | 1.0 | 1.000 | 1.132 |
| S3 Express One Zone | w1 | 200 B | 8 | 10.73 | 13.07 | 13.75 | 14.74 | 739 | 8.0 | 0.125 | 0.143 |
| S3 Express One Zone | w1 | 200 B | 64 | 11.23 | 13.93 | 19.97 | 19.97 | 5343 | 62.5 | 0.016 | 0.019 |
| S3 Express One Zone | w1 | 1024 B | 1 | 9.47 | 12.49 | 13.83 | 37.19 | 101 | 1.0 | 1.000 | 1.137 |
| S3 Express One Zone | w1 | 1024 B | 8 | 11.09 | 12.65 | 13.34 | 14.09 | 730 | 8.0 | 0.125 | 0.148 |
| S3 Express One Zone | w1 | 1024 B | 64 | 11.07 | 14.83 | 17.65 | 17.67 | 5477 | 62.5 | 0.016 | 0.025 |
| S3 Express One Zone | w20 | 200 B | 1 | 28.52 | 31.76 | 32.57 | 33.59 | 35 | 1.0 | 1.000 | 1.132 |
| S3 Express One Zone | w20 | 200 B | 8 | 30.29 | 32.51 | 33.55 | 33.70 | 265 | 8.0 | 0.125 | 0.143 |
| S3 Express One Zone | w20 | 200 B | 64 | 29.92 | 32.20 | 35.08 | 35.09 | 2083 | 62.5 | 0.016 | 0.019 |
| S3 Express One Zone | w20 | 1024 B | 1 | 28.50 | 31.71 | 32.61 | 47.59 | 35 | 1.0 | 1.000 | 1.137 |
| S3 Express One Zone | w20 | 1024 B | 8 | 29.93 | 32.01 | 33.37 | 34.02 | 266 | 8.0 | 0.125 | 0.148 |
| S3 Express One Zone | w20 | 1024 B | 64 | 31.05 | 33.18 | 33.54 | 33.55 | 2031 | 62.5 | 0.016 | 0.025 |
| S3 Express One Zone | w5 | 200 B | 1 | 13.55 | 16.32 | 17.48 | 27.18 | 72 | 1.0 | 1.000 | 1.132 |
| S3 Express One Zone | w5 | 200 B | 8 | 15.09 | 17.33 | 18.31 | 18.36 | 528 | 8.0 | 0.125 | 0.143 |
| S3 Express One Zone | w5 | 200 B | 64 | 14.40 | 17.36 | 17.71 | 17.73 | 4198 | 62.5 | 0.016 | 0.019 |
| S3 Express One Zone | w5 | 1024 B | 1 | 13.54 | 16.50 | 17.57 | 18.51 | 72 | 1.0 | 1.000 | 1.137 |
| S3 Express One Zone | w5 | 1024 B | 8 | 14.57 | 17.30 | 18.45 | 19.02 | 539 | 8.0 | 0.125 | 0.148 |
| S3 Express One Zone | w5 | 1024 B | 64 | 15.64 | 17.40 | 31.82 | 31.84 | 3876 | 62.5 | 0.016 | 0.025 |
| DynamoDB on-demand | perwrite | 200 B | 1 | 3.17 | 3.52 | 3.85 | 8.54 | 311 | 1.0 | 1.000 | 0.707 |
| DynamoDB on-demand | perwrite | 200 B | 8 | 3.67 | 5.75 | 7.72 | 9.58 | 2014 | 1.0 | 1.000 | 0.707 |
| DynamoDB on-demand | perwrite | 200 B | 64 | 10.18 | 137.54 | 168.25 | 171.65 | 2455 | 1.0 | 1.193 | 0.707 | DynamoDB throttled attempts: 290
| DynamoDB on-demand | perwrite | 1024 B | 1 | 2.47 | 2.80 | 3.32 | 17.96 | 395 | 1.0 | 1.000 | 1.419 |
| DynamoDB on-demand | perwrite | 1024 B | 8 | 3.36 | 30.55 | 31.47 | 33.82 | 695 | 1.0 | 1.254 | 1.422 | DynamoDB throttled attempts: 381
| DynamoDB on-demand | perwrite | 1024 B | 64 | 168.30 | 311.24 | 338.33 | 341.44 | 506 | 1.0 | 2.194 | 1.430 | DynamoDB throttled attempts: 1791
| DynamoDB on-demand | w1 | 200 B | 1 | 5.28 | 5.71 | 6.18 | 10.04 | 188 | 1.0 | 1.000 | 0.707 |
| DynamoDB on-demand | w1 | 200 B | 8 | 5.68 | 6.12 | 6.95 | 7.84 | 1394 | 8.0 | 0.125 | 0.178 |
| DynamoDB on-demand | w1 | 200 B | 64 | 5.69 | 6.03 | 6.21 | 6.23 | 11051 | 62.5 | 0.016 | 0.156 |
| DynamoDB on-demand | w1 | 1024 B | 1 | 4.94 | 5.33 | 6.31 | 26.33 | 199 | 1.0 | 1.000 | 1.419 |
| DynamoDB on-demand | w1 | 1024 B | 8 | 5.17 | 5.71 | 6.03 | 6.08 | 1533 | 8.0 | 0.125 | 0.803 |
| DynamoDB on-demand | w1 | 1024 B | 64 | 7.37 | 82.11 | 82.29 | 82.30 | 2380 | 62.5 | 0.025 | 0.741 | DynamoDB throttled attempts: 13
| DynamoDB on-demand | w20 | 200 B | 1 | 23.95 | 24.54 | 25.00 | 34.13 | 42 | 1.0 | 1.000 | 0.707 |
| DynamoDB on-demand | w20 | 200 B | 8 | 24.64 | 25.18 | 25.81 | 25.93 | 324 | 8.0 | 0.125 | 0.178 |
| DynamoDB on-demand | w20 | 200 B | 64 | 24.47 | 25.05 | 25.11 | 25.11 | 2550 | 62.5 | 0.016 | 0.156 |
| DynamoDB on-demand | w20 | 1024 B | 1 | 24.12 | 24.68 | 25.31 | 38.32 | 41 | 1.0 | 1.000 | 1.419 |
| DynamoDB on-demand | w20 | 1024 B | 8 | 24.44 | 25.09 | 25.73 | 32.13 | 325 | 8.0 | 0.125 | 0.803 |
| DynamoDB on-demand | w20 | 1024 B | 64 | 26.62 | 28.69 | 28.93 | 28.95 | 2328 | 62.5 | 0.016 | 0.736 |
| DynamoDB on-demand | w5 | 200 B | 1 | 9.38 | 9.89 | 10.48 | 21.61 | 106 | 1.0 | 1.000 | 0.707 |
| DynamoDB on-demand | w5 | 200 B | 8 | 8.79 | 9.14 | 9.76 | 9.78 | 906 | 8.0 | 0.125 | 0.178 |
| DynamoDB on-demand | w5 | 200 B | 64 | 9.60 | 10.00 | 10.80 | 10.80 | 6455 | 62.5 | 0.016 | 0.156 |
| DynamoDB on-demand | w5 | 1024 B | 1 | 9.11 | 9.66 | 10.35 | 21.88 | 109 | 1.0 | 1.000 | 1.419 |
| DynamoDB on-demand | w5 | 1024 B | 8 | 9.29 | 9.83 | 10.50 | 10.57 | 855 | 8.0 | 0.125 | 0.803 |
| DynamoDB on-demand | w5 | 1024 B | 64 | 10.90 | 84.03 | 85.12 | 85.14 | 2385 | 62.5 | 0.023 | 0.740 | DynamoDB throttled attempts: 11
| EBS gp3 + fdatasync | w0 | 200 B | 1 | 0.47 | 0.56 | 0.63 | 0.75 | 2107 | 1.0 | 1.000 | 0.000 |
| EBS gp3 + fdatasync | w0 | 200 B | 8 | 0.91 | 1.04 | 1.15 | 1.31 | 8629 | 4.0 | 0.251 | 0.000 |
| EBS gp3 + fdatasync | w0 | 200 B | 64 | 0.99 | 1.09 | 1.15 | 1.16 | 63546 | 31.9 | 0.031 | 0.000 |
| EBS gp3 + fdatasync | w0 | 1024 B | 1 | 0.46 | 0.59 | 0.66 | 0.79 | 2113 | 1.0 | 1.000 | 0.000 |
| EBS gp3 + fdatasync | w0 | 1024 B | 8 | 0.93 | 1.04 | 1.10 | 1.12 | 8496 | 4.0 | 0.251 | 0.000 |
| EBS gp3 + fdatasync | w0 | 1024 B | 64 | 1.17 | 1.44 | 1.47 | 1.48 | 53258 | 32.6 | 0.031 | 0.000 |
| 3-AZ quorum (2 of 3) | w0 | 200 B | 1 | 0.96 | 1.09 | 1.19 | 1.79 | 1030 | 1.0 | 0.000 | 0.011 |
| 3-AZ quorum (2 of 3) | w0 | 200 B | 8 | 1.94 | 2.16 | 2.27 | 2.50 | 4079 | 4.0 | 0.000 | 0.009 |
| 3-AZ quorum (2 of 3) | w0 | 200 B | 64 | 1.81 | 1.95 | 2.00 | 2.01 | 35690 | 32.6 | 0.000 | 0.008 |
| 3-AZ quorum (2 of 3) | w0 | 1024 B | 1 | 1.06 | 1.19 | 1.30 | 2.07 | 929 | 1.0 | 0.000 | 0.042 |
| 3-AZ quorum (2 of 3) | w0 | 1024 B | 8 | 1.69 | 1.86 | 1.98 | 2.12 | 4690 | 4.0 | 0.000 | 0.039 |
| 3-AZ quorum (2 of 3) | w0 | 1024 B | 64 | 2.40 | 2.90 | 3.40 | 3.40 | 26738 | 33.3 | 0.000 | 0.039 |

## Many databases (open loop)

200 databases, each 1.0 write/s (Poisson), 1024 B, latency from the scheduled time. `perdb` = one log per database; `shared` = one batch for all databases.

| backend | layout | variant | writes | p50 | p99 | max | commits / write | variable USD / M |
|---|---|---|---|---|---|---|---|---|
| S3 Standard | perdb | perwrite | 12154 | 24.94 | 93.66 | 313.19 | 1.000 | 5.001 |
| S3 Standard | shared | w1 | 12033 | 26.50 | 70.74 | 186.47 | 0.764 | 3.819 |
| S3 Standard | shared | w20 | 12100 | 37.96 | 63.31 | 110.95 | 0.194 | 0.970 |
| S3 Standard | shared | w5 | 12016 | 29.34 | 65.96 | 111.06 | 0.472 | 2.362 |
| S3 Express One Zone | perdb | perwrite | 12024 | 10.42 | 14.16 | 32.66 | 1.000 | 1.137 |
| S3 Express One Zone | shared | w1 | 11850 | 10.07 | 12.18 | 15.73 | 0.771 | 0.878 |
| S3 Express One Zone | shared | w20 | 12054 | 22.02 | 32.45 | 36.20 | 0.195 | 0.227 |
| S3 Express One Zone | shared | w5 | 11924 | 13.54 | 17.19 | 29.32 | 0.480 | 0.549 |
| DynamoDB on-demand | perdb | perwrite | 12144 | 4.03 | 5.91 | 42.97 | 1.000 | 1.419 |
| DynamoDB on-demand | shared | w1 | 12039 | 5.77 | 7.71 | 19.66 | 0.764 | 1.253 |
| DynamoDB on-demand | shared | w20 | 12171 | 18.55 | 27.78 | 43.52 | 0.193 | 0.851 |
| DynamoDB on-demand | shared | w5 | 12021 | 9.27 | 11.54 | 27.62 | 0.475 | 1.049 |

## Crash test (kill -9 of the writer, restart, verify)

Per cycle: random variant, concurrency (1/8/64), size (200 B / 1 KB), kill after a random 150–1500 ms of writing; a new process reads the log back. An id counts as acknowledged only after the harness read its ack line (strictly after the ack). Quorum: the read uses a random pair of the 3 nodes.

| backend | cycles | acknowledged writes | lost | corrupt batches | bad payloads | harness failures | recovery p50 / max (ms) |
|---|---|---|---|---|---|---|---|
| S3 Standard | 200 / 200 | 46252 | 0 | 0 | 0 | 0 | 158 / 984 |
| S3 Express One Zone | 200 / 200 | 45248 | 0 | 0 | 0 | 0 | 118 / 402 |
| DynamoDB on-demand | 200 / 200 | 73176 | 0 | 0 | 0 | 0 | 79 / 176 |
| EBS gp3 + fdatasync | 200 / 200 | 86743 | 0 | 0 | 0 | 0 | 2 / 11 |
| 3-AZ quorum (2 of 3) | 200 / 200 | 83911 | 0 | 0 | 0 | 1 | 8 / 43 |

A torn last frame of a local log after kill -9 is expected (never acknowledged) and is not counted as corruption. kill -9 kills the process, not the machine: for EBS the page cache survives it, so the machine-loss case is the reattach test below.

## Quorum: a node killed, a stale node returning

8 writers, 1 KB, group commit. Acknowledged 107162, recovered from nodes 1+2 only: 107162, **lost 0**.

- 15.0 s: node1 SIGKILL — `{"restart_after_s": 15.0}`
- 34.8 s: stale check (node1 back, held) — `{"negative_control": {"acked_missing_on_node1_alone": 33166, "acked_so_far": 78745, "on_node1_alone": 45579}, "positive": {"acked_missing_on_nodes_0_2": 0}}`
- 36.0 s: node1 hold released (catch-up starts) — `{"head_seq": 20168, "node1_durable_seq": 11387}`
- 36.4 s: node1 caught up — `{"batches_behind": 8781, "catch_up_ms": 379.038569}`
- 45.0 s: node0 SIGKILL — `{"restart_after_s": 15.0}`
- 58.0 s: writes stopped — `{"acked": 107162}`

| phase | acks | p50 | p99 | max |
|---|---|---|---|---|
| all_up_0_15 | 45571 | 2.76 | 3.05 | 161.75 |
| node0_down_45_58 | 14783 | 6.74 | 7.87 | 16.40 |
| node1_catching_up_36_45 | 10977 | 6.55 | 7.41 | 14.12 |
| node1_down_15_30 | 27058 | 2.97 | 7.14 | 13.47 |
| node1_stale_held_30_36 | 6807 | 6.72 | 21.03 | 24.96 |

## EBS: force-detach while writing, attach to the other instance, resume

| cycle | from→to | detach call → available (s) | attach call → attached (s) | attach call → device visible (s) | mount (ms) | log recovery (ms) | first ack (ms) | detach call → first ack (s) | acked before | lost |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | A→B | 1.5 | 2.6 | 1.7 | 33 | 729 | 73 | 4.1 | 111146 | 0 |
| 1 | B→A | 1.5 | 1.2 | 0.8 | 32 | 1432 | 95 | 3.9 | 137886 | 0 |
| 2 | A→B | 1.5 | 1.3 | 0.8 | 28 | 1436 | 111 | 3.9 | 169406 | 0 |
| 3 | B→A | 1.4 | 1.3 | 0.7 | 33 | 1504 | 132 | 3.8 | 200998 | 0 |
| 4 | A→B | 1.4 | 1.2 | 0.8 | 32 | 1489 | 153 | 3.9 | 234479 | 0 |
| 5 | B→A | 1.9 | 1.2 | 0.9 | 126 | 1747 | 174 | 4.8 | 259543 | 0 |

times *_s on the runner clock, *_ms on the instance clock; cross-clock values assume NTP-synced clocks. Both instances were already running (warm standby); a cold start would add the instance launch.

## Fixed costs (EBS, quorum) at the fetched prices

- EBS log volume (20 GB gp3, baseline IOPS/throughput): 0.0024 USD/h. Equal to 1.5 USD per million writes at 0.45 writes/s; below that rate it costs more per write.
- Quorum log nodes as measured (3 × c6i.large + public IPv4 + 10 GB root): 0.2922 USD/h, = 1.5 USD/M at 54 writes/s. If the three logs live on machines that exist anyway (the database replicas), only the inter-AZ transfer is extra.

## Prices used (eu-west-1, fetched at run time)

Fetched 2026-10-30T07:00:09Z. Public offer files (publication dates below) and the AWS Price List Query API; the human-readable pages are listed for reference.

| item | USD | unit | source |
|---|---|---|---|
| s3.std_put: $0.005 per 1,000 PUT, COPY, POST, or LIST requests | 5e-06 | Requests | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| s3.std_get: $0.004 per 10,000 GET and all other requests | 4e-07 | Requests | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| s3.std_storage: $0.023 per GB - first 50 TB / month of storage used | 0.023 | GB-Mo | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| s3.xz_put: $0.00113 per 1,000 PUT Requests/COPY/POST or LIST Requests to Express One Zone | 1.13e-06 | Requests | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| s3.xz_get: $0.00003 per 1,000 GET and All Other Requests to Express One Zone | 3e-08 | Requests | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| s3.xz_upload_gb: $0.0032 per GB for Object Upload in Express One Zone | 0.0032 | GB | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| s3.xz_retrieval_gb: $0.0006 per GB for Object Retrieval in Express One Zone | 0.0006 | GB | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| s3.xz_storage: $0.11 per GB-Mo of Storage in Express One Zone  | 0.11 | GB-Mo | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json (published 2026-10-15T17:35:40Z) |
| dynamodb.wru: $0.705 per million write request units (EU (Ireland)) | 7.05e-07 | WriteRequestUnits | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDynamoDB/current/eu-west-1/index.json (published 2026-09-11T12:44:22Z) |
| dynamodb.rru: $0.1415 per million read request units (EU (Ireland)) | 1.415e-07 | ReadRequestUnits | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDynamoDB/current/eu-west-1/index.json (published 2026-09-11T12:44:22Z) |
| dynamodb.storage: $0.283 per GB-Month of storage used beyond first 25 free GB-Months | 0.283 | GB-Mo | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDynamoDB/current/eu-west-1/index.json (published 2026-09-11T12:44:22Z) |
| vpc.ipv4_in_use_hour: $0.005 per In-use public IPv4 address per hour | 0.005 | Hrs | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonVPC/current/eu-west-1/index.json (published 2026-10-16T22:43:21Z) |
| transfer.inter_az_gb: $0.010 per GB - regional data transfer - in/out/between EC2 AZs or using elastic IPs or ELB | 0.01 | GB | https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/eu-west-1/index.json (published 2026-09-16T13:22:08Z) |
| ec2.c6i_large_hour: $0.0912 per On Demand Linux c6i.large Instance Hour | 0.0912 | Hrs | AWS Price List Query API pricing:GetProducts ServiceCode=AmazonEC2 filters={'regionCode': 'eu-west-1', 'instanceType': 'c6i.large', 'operatingSystem': 'Linux', 'tenancy': 'Shared', 'preInstalledSw': 'NA', 'capacitystatus': 'Used', 'licenseModel': 'No License required'} (published 2026-10-29T17:56:34Z) |
| ec2.gp3_gb_month: $0.088 per GB-month of General Purpose (gp3) provisioned storage - EU (Ireland) | 0.088 | GB-Mo | AWS Price List Query API pricing:GetProducts ServiceCode=AmazonEC2 filters={'regionCode': 'eu-west-1', 'volumeApiName': 'gp3', 'productFamily': 'Storage'} (published 2026-10-29T17:56:34Z) |

Pages: s3: https://aws.amazon.com/s3/pricing/; express: https://aws.amazon.com/s3/storage-classes/express-one-zone/ (prices on https://aws.amazon.com/s3/pricing/); dynamodb: https://aws.amazon.com/dynamodb/pricing/on-demand/; ec2: https://aws.amazon.com/ec2/pricing/on-demand/; ebs: https://aws.amazon.com/ebs/pricing/; vpc_ipv4: https://aws.amazon.com/vpc/pricing/; data_transfer: https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer_within_the_same_AWS_Region

## Spend and clean-up

Live spend log `results-walbench/cost_live.csv` (90 rows, every 20 s). Last estimate: **0.73 USD** (cap 5.0, automatic stop at 4.0). This is computed from counted requests and instance/volume time at list prices, with margins; AWS billing data lags by about a day and was not read.

Teardown verify (2026-10-30T07:48:44Z): **RESOURCES LEFT: [][{"kind": "ec2:network-interface", "id": "eni-071692f5f37da0073"}, {"kind": "ec2:volume", "id": "vol-07c6963b684966cd0"}, {"kind": "ec2:volume", "id": "vol-022a9dfb8242493a5"}, {"kind": "ec2:network-interface", "id": "eni-0ad4f47a2948599d7"}, {"kind": "ec2:volume", "id": "vol-02b03a6a37920584c"}, {"kind": "ec2:network-interface", "id": "eni-084447171ff3c000b"}, {"kind": "ec2:volume", "id": "vol-09b207da3adf9693a"}, {"kind": "ec2:volume", "id": "vol-08b78b6b449cf6d7c"}, {"kind": "ec2:network-interface", "id": "eni-0aaf915998c1ce546"}]** (instances, volumes, security groups, network interfaces, S3 buckets, directory buckets, DynamoDB tables, plus the Resource Groups Tagging API for tag project=fenvix-walbench).

## What was not measured

- Machine crash of the writer for S3 / Express / DynamoDB / quorum (the writer is killed with kill -9; for these backends the ack comes from a remote service, so a process kill and a machine loss look the same to the log). For EBS the machine-loss case is the force-detach test.
- Loss of an AZ, of the Express directory bucket's AZ, or service-side incidents; long runs (each cell is ~1500 writes; p99 from a short window).
- Writes larger than 1 KB, read latency, compaction or truncation of the log, cost of retaining the log beyond 24 h.
- The quorum protocol is benchmark grade (no epochs/fencing, catch-up from the writer's buffer); a production protocol adds work not measured here.
- Group-commit windows are timer based (tokio, 1 ms resolution): a 1 ms window waits 1–2 ms.
- DynamoDB throttled attempts over all cells: 2486 (an on-demand table starts at about 4000 WRU/s; throttled attempts are retried, shown per cell, and not billed in the cost figures).
- S3 Express `CreateSession` calls are made by the SDK and are not counted (one per bucket per ~5 minutes).
