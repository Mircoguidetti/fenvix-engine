# P3, the front door: measures (2026-11-02)

Machine: 2 vCPU (Intel Xeon @ 2.10GHz), 7 GB RAM, open files hard limit 20,000 per process (cannot be raised here).
Server pinned to core 0 (`taskset -c 0`, one tokio worker), generator (`loadgen/src/bin/porta.rs`, official Rust driver
mongodb 3.9.1, rustls) and the fake S3 of the ctl-tests (`tools/sort-top1/run_fake_s3.py`) on core 1. `--auth`,
`--tls-mode requireTLS`, ECDSA P-256 certificate (the infra issues ECDSA certificates from Let's Encrypt), SCRAM-SHA-256,
1000 databases with one user each. The driver keeps the salted password per user in a process-wide cache: PBKDF2 runs once
per user in the warm-up, not per connection. Every number is indicative: two cores, and the machine speed moved by about
10% during the day (same binary 0.84-0.94 ms per connection).

Idle connections (`tools/porta/hold.sh`): 19,500 connections held (two generators: 500 clients x 18 + 500 x 17 pooled and
authenticated, plus 2 monitoring connections per client), RSS of the server before (no client) and after.
* main (71a435e), TLS: 81,412 kB -> 961,964 kB: 45.2 KB per connection.
* this branch (task future boxed), TLS: 86,860 kB -> 519,328 kB: 22.2 KB per connection.
* this branch, no TLS: 84,092 kB -> 370,372 kB: 14.7 KB per connection (main without TLS: 37.6 KB).
* this branch, TLS, 127.0.0.1 NOT trusted (`--trusted-networks ""`), default limits: 19,500 open, 2,000 not
  authenticated, 0 refused, 22.2 KB per connection.
* The task future of a connection: 22,400 bytes on main, 1,616 bytes with the command future boxed (`size_of_val`).

New connections (`tools/porta/churn_mode.sh`): each read finds its pooled connection idle (maxIdleTimeMS 1) and opens a new
one: TCP + TLS + hello + saslStart + saslContinue + find, then the connection is closed. Server CPU (user + system from
/proc) over 30 s divided by the connections created. Background (10 per second): 1.1-1.4 s per 30 s.
* 800/s, TLS, resumption off / cache (default) / tickets: 0.853 and 0.857 / 0.877 and 0.837 / 0.806 and 0.806 ms per
  connection (morning); later the same day: main 0.942 and 0.932, branch cache 0.936 and 0.965, off 0.946, tickets 0.938
  and 0.997. Zero errors in every run.
* 800/s without TLS: 0.602 and 0.582 ms.
* 2000/s asked, TLS: server at 97% of its core and generator at 91-96%: 1,273-1,559 connections per second reached,
  0.61-0.76 ms per connection, zero errors, p50 latency 1.2-1.5 s (queueing on both cores).
* Session resumption works with `tickets` (openssl s_client -sess_in: "Reused, TLSv1.3"); with `cache` it works for one
  client but under 1000 clients the 256-session cache turns over in a fraction of a second, so cache = off in practice.

Raw output: the `run-*.log`, `hold-*.log` and `natbig*.log` files here (the hold numbers before/after are in the report).
