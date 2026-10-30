# journal3z: library or custom

## Criteria (fixed before any measurement; this section was committed before the spikes ran)

Question: implement the protocol of `docs/journal3z-design.md` (D1) on top of an existing Rust consensus/replication
library, or write it ourselves on top of a published algorithm. Candidates: **openraft**, **raft-rs (tikv `raft`)**,
**omnipaxos**, and **custom** (Raft log replication with epochs from the lease, `journal3z/core`). Reference point:
**proto** = the walbench quorum write path (send to 3, fdatasync, ack at 2 of 3; no epochs, no fencing), run under
the same simulated conditions, so that "not below the prototype" is compared like for like.

Common test bed (`journal3z/spikes/harness`), identical for every candidate:
- 3 nodes in one process. Every message between two different nodes goes through one **delay line** (a dedicated
  thread with a precise timer, not tokio's 1 ms timer wheel) that delays it by a one-way delay drawn uniformly per
  message. **Primary network: one-way 0.5–1.0 ms (round trip 1–2 ms)**, our reading of "1–2 ms inter-node delay";
  secondary (reported, not judged): one-way 1–2 ms (round trip 2–4 ms).
- Each node writes its log to its own real file with `pwrite`-style appends and **`fdatasync` before it reports an
  entry durable** (one shared group-commit helper, so every candidate has the same disk behaviour).
- The client (writer) is co-located with node 0 (as walbench's node 0 on the client instance): no delay to node 0.
- Load: 1 KB payloads, closed loop, **8 concurrent writers**, 2,000 warm-up writes, then 20,000 measured writes;
  latency of each write from submit to "acknowledged" (committed on 2 of 3 after fsync). 3 runs; the median run is
  reported, all 3 are kept in the raw JSON.

| # | criterion | pass if | how measured |
|---|---|---|---|
| C1 | p99 acknowledged 1 KB write, conc 8, primary network | **≤ 5.0 ms** | latency histogram over the 20,000 measured writes |
| C2 | throughput per group, conc 8, primary network | **≥ 4,700 writes/s**, *or*, if the network floor makes that impossible for every candidate, **≥ 90 % of `proto` measured in the same run set** | writes / wall time. Note fixed in advance: at conc 8 a closed loop gives ≈ 8 / mean latency, so 4,700/s needs mean latency ≤ 1.70 ms while the simulated mean round trip alone is 1.5 ms; the relative form is therefore expected to be the deciding one. Also reported: conc 64. |
| C3 | CPU and memory per server | CPU ≤ **2× `proto`** per acknowledged write (process CPU time, all 3 nodes, from `getrusage`) and ≤ 50 µs per write; peak RSS ≤ **256 MB** under the C1 load | `getrusage(RUSAGE_SELF)` and `/proc/self/status` VmHWM |
| C4 | one shared group vs many groups | with **1,000 idle groups** (or logical logs) on 3 nodes: ≤ **1,000 messages/s** in total and ≤ **5 %** of one core of CPU; RSS ≤ **512 MB** | 10 s idle after start-up, delay-line message counter, CPU time delta |
| C5 | fencing tied to an external lease | the candidate lets a writer whose epoch comes from outside (the lease) make every older writer unable to commit, **without a second, independent election**; demonstrated by a spike test (stale writer's append refused after the new writer's fence) | spike test + reading of the API; pass / partial / fail |
| C6 | snapshots / log compaction | per-group (per-database) trim to an externally given index (the S3 marker), without the library forcing a state-machine snapshot transfer we do not need | API reading + spike; pass / partial / fail |
| C7 | maintenance and maturity | a release in the last 12 months, a stable (non-alpha) line usable today, production users, active repository | crates.io data fetched on the day (versions, dates, downloads); repository activity where reachable |
| C8 | storage layer to adapt | reported as lines of adapter code in the spike + what remains for production (a fact, ranked, not pass/fail) | `wc -l` of each adapter |

Decision rule (fixed in advance): a library is recommended only if it passes C1, C2, C5 and C6 (the protocol cannot be
built without C5/C6) and does not fail C4; among those, the one with the best C7 then C8. If none qualifies, custom is
recommended **only if** the custom spike itself passes C1–C4; otherwise the report says so and recommends the next
step. Every number is labelled measured (this container: 4 vCPU, virtio disk) or estimated. The container's disk and
CPU are not an AWS c6i.large with gp3: absolute numbers are for comparison between candidates, not predictions for AWS.

## Results

All numbers in this section are **measured** in this container (4 vCPU, virtio disk with ext4, fdatasync p50 0.14 ms /
p99 0.6 ms measured with a 1 KB append loop) on 2026-10-30, by `journal3z/spikes/bench.sh`, run sequentially on an
otherwise idle machine. Raw JSON: `journal3z/results/spikes/`. Test-bed accuracy (measured): the delay line adds about
60 µs per hop (configured one-way 0.5–1.0 ms → measured round trip mean 1.63 ms, ideal 1.50; p99 2.24 ms).

Versions: openraft 0.9.25 (crates.io, 2026-07-28; storage-v2), raft 0.7.0 (crates.io, 2023-03-07; protobuf-codec),
omnipaxos 0.2.4 (crates.io, 2026-10-18). Spike code and each spike's notes: `journal3z/spikes/<name>/` (`NOTES.md`).
The three library spikes were written by sub-agents to the same specification and the same test bed; I ran every
final measurement myself. Each adapter persists through the common `LogFile` (fdatasync before any acknowledgement)
and sends every message between different nodes through the delay line; the writer is co-located with node 0, which is
the leader.

### Latency and throughput, conc 8, 1 KB, one-way delay 0.5–1.0 ms (3 runs; median run by p99; all 3 p99 shown)

| candidate | p50 ms | p95 ms | p99 ms (median run) | p99 of runs 1/2/3 | max ms | writes/s | CPU µs/write (excl. net) | CPU µs/write (all) | peak RSS MB | msgs/write | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|
| proto | 2.89 | 4.25 | **5.37** | 5.83 / 4.85 / 5.37 | 45.5 | 2660 | 116 | 173 | 4 | 0.72 | 0 |
| custom | 1.99 | 2.51 | **3.77** | 3.62 / 3.77 / 3.87 | 12.6 | 3920 | 202 | 341 | 33 | 3.13 | 0 |
| openraft (1 entry per write) | 4.91 | 6.51 | **8.00** | 7.81 / 8.00 / 8.17 | 13.5 | 1581 | 290 | 340 | 19 | 1.10 | 0 |
| openraft (batched entries) | 7.63 | 9.04 | **10.56** | 10.75 / 10.21 / 10.56 | 15.0 | 1157 | 228 | 305 | 34 | 1.72 | 0 |
| raft-rs | 2.22 | 2.73 | **3.96** | 3.44 / 3.96 / 4.37 | 10.1 | 3515 | 259 | 440 | 85 | 8.08 | 0 |
| omnipaxos (BLE on) | 2.04 | 2.54 | **3.77** | 3.77 / 3.79 / 3.75 | 16.9 | 3804 | 200 | 320 | 29 | 4.04 | 0 |
| omnipaxos (BLE off) | 2.08 | 2.62 | **3.88** | 4.07 / 3.88 / 3.84 | 25.9 | 3729 | 212 | 331 | 29 | 4.02 | 0 |

### Concurrency 64 and secondary network (one-way 1–2 ms), one run each

| candidate | conc 64: p50 / p99 ms | conc 64: writes/s | conc 64: CPU µs/write excl. net | delay 1–2 ms, conc 8: p50 / p99 ms | writes/s |
|---|---|---|---|---|---|
| proto | 3.38 / 5.44 | 18494 | 27 | 5.14 / 8.01 | 1536 |
| custom | 2.40 / 3.99 | 26204 | 40 | 3.49 / 5.58 | 2264 |
| openraft (1 entry per write) | 19.94 / 29.41 | 3132 | 137 | 7.99 / 11.54 | 962 |
| openraft (batched) | 8.02 / 10.47 | 8192 | 41 | 13.64 / 17.92 | 648 |
| raft-rs | 2.97 / 6.22 | 20630 | 65 | 3.95 / 6.03 | 1996 |
| omnipaxos (BLE on) | 2.54 / 4.76 | 24483 | 40 | 3.67 / 5.67 | 2153 |
| omnipaxos (BLE off) | 2.55 / 3.93 | 24689 | 39 | – | – |

### Idle groups (10 s after start-up; messages through the delay line; CPU of the whole process and without the delay-line thread)

| candidate | groups | start-up s | messages/s | CPU % of one core (excl. net) | RSS MB |
|---|---|---|---|---|---|
| proto | 1 | 0.00 | 0 | 0.0 (0.0) | 3 |
| proto | 100 | 0.00 | 0 | 0.0 (0.0) | 7 |
| proto | 1000 | 0.00 | 0 | 0.0 (0.0) | 36 |
| custom | 1 | 0.00 | 0 | 0.4 (0.4) | 4 |
| custom | 100 | 0.00 | 0 | 0.4 (0.4) | 4 |
| custom | 1000 | 0.02 | 0 | 0.6 (0.6) | 7 |
| openraft | 1 | 0.08 | 52 | 1.0 (0.8) | 6 |
| openraft | 100 | 0.12 | 5239 | 13.8 (10.0) | 34 |
| openraft | 1000 | 1.12 | 52452 | 122.6 (87.6) | 288 |
| raft-rs | 1 | 0.01 | 134 | 2.1 (1.5) | 5 |
| raft-rs | 100 | 0.01 | 13225 | 11.1 (4.9) | 6 |
| raft-rs | 1000 | 0.03 | 133306 | 52.2 (35.7) | 17 |
| omnipaxos (BLE on) | 1 | 0.11 | 120 | 1.0 (0.5) | 4 |
| omnipaxos (BLE on) | 100 | 0.11 | 11999 | 9.7 (5.7) | 12 |
| omnipaxos (BLE on) | 1000 | 0.17 | 119989 | 89.2 (52.8) | 84 |
| omnipaxos (BLE off) | 1 | 0.01 | 0 | 0.2 (0.2) | 4 |
| omnipaxos (BLE off) | 100 | 0.02 | 0 | 1.5 (1.5) | 10 |
| omnipaxos (BLE off) | 1000 | 0.12 | 0 | 14.5 (14.5) | 64 |

Notes on the measurements:
- openraft 0.9.25 makes the leader fdatasync once per `client_write` and waits for it before handling anything else
  (spike notes, with source references). The batched variant (one entry carries every write queued while the previous
  `client_write` is in flight) was added for fairness; with one entry in flight it is slower at concurrency 8 and much
  faster at 64. 0.10 (alpha) changes the leader's write path; not measured (alpha).
- raft-rs: tick 10 ms, election 10 ticks, heartbeat 3 ticks, check-quorum and pre-vote on, `max_size_per_msg` 1 MiB
  (the default 0 means one entry per message). Acknowledged when committed (the leader counts itself only after its
  own fsync). 8 messages per write: raft-rs broadcasts the commit index in extra messages.
- omnipaxos: tick 100 ms. "BLE off" disables its own leader election (the mode our design would use: the leader comes
  from the lease); with BLE off and 1,000 groups it still spends 14.5 % of a core on 3,000 instances' ticks (the spike
  notes report 0.9 % with a 1 s tick, smoke run, not re-measured).
- custom: `journal3z/core` driven in real time; pipelined appends; one batch per node per flush. Its idle cost is the
  20 ms retransmission timer of the writer, which only does work for databases with something outstanding.
- CPU "excl. net" removes the delay-line thread (the simulated network belongs to no candidate). At concurrency 8 most
  CPU is per-message overhead of the in-process test bed (channel hops, thread wake-ups); see C3.

### Fencing (C5) and trim (C6) scenarios

| candidate | C5: external epoch fences an older writer | C6: trim to an externally given index |
|---|---|---|
| custom | `--mode fence` (real nodes, delay line, fdatasync, two writer processes): A (epoch 10) 100/100 acknowledged; B opens with epoch 11 and replays exactly those 100; A's next 100 writes: **0 acknowledged, 100 failed "fenced by epoch 11"**; B 100/100. No election exists to begin with. **Pass.** | `uploaded(k)` → TRIM(k); catch-up skips everything ≤ k (simulator, 360 M steps). Per database. **Pass.** |
| openraft 0.9.25 | The term is internal; a new election always uses term + 1; no API sets it; 0.9 has no leader transfer. With elections off and forced elections: S1 the lease holder never became leader (20 attempts, its log was behind); S2 the term can be pushed to the lease epoch only approximately (31 instead of 30); without an application-level guard a stale node won an election and committed; with a guard, a stale node's higher term still demoted the real leader. Raft's vote restriction means a lease holder whose log is behind can never lead (the design's rule R3 avoids that by reading the log from 2 nodes). **Fail.** | Purge only below a snapshot; a pointer snapshot ("S3 marker k", 154 bytes) built at k worked, and a follower behind the purge point received it (2 InstallSnapshot RPCs). **Partial** (works, but through the snapshot machinery). |
| raft-rs 0.7.0 | `become_follower(seq − 1)` + a self-sent `MsgTimeoutNow` makes the lease holder campaign at term = lease seq (worked: lease 9 → term 9); the stale leader's in-flight and later writes failed ("overwritten by a later term"). But a plain `propose` on the old writer after it stepped down is **forwarded to the new leader and committed** (0.7.0 cannot disable forwarding; master has `disable_proposal_forwarding`, unreleased), and a lease holder whose log is behind cannot win the vote (remedy: elect the up-to-date node, then transfer leadership: term 14 instead of 12). Uses an internal-looking public API. **Partial.** | `compact(k)` + a ~20-line `Storage::snapshot()` override: lagging follower got a 0-byte snapshot at k = 36, then entries 37–46; no state-machine data moved. **Pass** (per group). |
| omnipaxos 0.2.4 | No API sets a ballot; `try_become_leader` uses the current promise + 1. Native takeover: the old leader's late append returned Ok, was forwarded and **decided after the fence**; a stale node calling `try_become_leader` again took leadership back from the newer lease holder and its write was decided. A workaround with a forged internal `Prepare` message worked but is not supported API. **Fail.** | `trim(Some(150))` with one node down: `Err(NotAllDecided(100))`: one down node pins the log whatever S3 holds; the snapshot alternative sends state-machine snapshots to lagging nodes. **Fail.** |

### Maintenance and maturity (C7), adapter size (C8)

| candidate | C7 (facts fetched 2026-10-30 from crates.io; repository activity where reachable) | C8 adapter lines (`wc -l`) and what production still needs |
|---|---|---|
| openraft | stable 0.9.0 (2024-03) … 0.9.25 (2026-07-28), 4 releases in 2026; 0.10 only as alphas (37, latest 2026-10-10); 2.13 M downloads, 965 k in 90 days; production user named in its README: Databend. GitHub activity not checked (API not reachable from this session). **Pass.** | 422 (storage 243 + network 179); still: restart recovery, entry/membership encoding, segment files and reclamation, a transport batching across groups, a durable epoch guard and the lease integration, an S3-reading path for the pointer snapshot, exit on fsync error |
| raft-rs | last release 0.7.0 on 2023-03-07 (3.6 years); git master active but slow (29 unreleased commits since, 5 in 2026); TiKV uses git master, not the crates.io release; 890 k downloads. **Fail** (no release in 12 months). | 442; still: a durable log engine (truncation, recovery, segments, trim, erase), a batching transport, a multi-group driver with hibernation of idle groups, lease/term glue including the "lease holder behind" case, membership changes the D7 way |
| omnipaxos | 0.2.2 (2023-11), then nothing until 0.2.3 (2026-09-24) and 0.2.4 (2026-10-18), an API-breaking 0.2.3 without changelog; 33 k downloads; README says in development; research origin (KTH, EuroSys 2023); no production user found (not verified). **Fail.** | 608 (storage 234 + driver 374); still: on-disk log, serialization, reconnection handling, fsync-error handling, the C5 and C6 workarounds |
| custom | ours: maintenance and correctness are entirely our responsibility (the main risk, see the report). The algorithm is not new (Raft log rules + lease-assigned epochs + Vertical Paxos reconfiguration). | protocol core 667 lines without tests (shared by simulator, model checker and spike) + spike driver 337; still: node daemon (segments, CRC, fence checkpoint, restart), TCP transport, writer client with lease/S3 integration, engine integration (`Journal` trait) |

### Verdict per criterion

| criterion | proto (reference) | custom | openraft 0.9.25 | raft-rs 0.7.0 | omnipaxos 0.2.4 |
|---|---|---|---|---|---|
| C1 p99 ≤ 5 ms (conc 8, primary network) | 5.37 ms — fail | **3.77 — pass** | 8.00 (batched 10.56) — fail | 3.96 — pass | 3.77 (BLE off 3.88) — pass |
| C2 throughput: 4,700/s impossible for every candidate at conc 8 (as predicted when the criteria were fixed) → ≥ 90 % of proto (2,394/s) | 2,660/s | **3,920/s (147 %) — pass** | 1,581 (59 %) / batched 1,157 — fail | 3,515 (132 %) — pass | 3,804 (143 %) — pass |
| C3 CPU ≤ 2× proto (232 µs) **and** ≤ 50 µs per write; RSS ≤ 256 MB | 116 µs; 4 MB | 202 µs (1.74×), RSS 33 MB: relative and memory pass, **absolute fail** | 290 µs (2.5×) — fail | 259 µs (2.23×) — fail | 200 µs (1.72×): relative pass, absolute fail |
| C4 1,000 idle groups: ≤ 1,000 msg/s, ≤ 5 % CPU, ≤ 512 MB | 0 msg/s | **0 msg/s, 0.6 %, 7 MB — pass** | 52,452 msg/s, 123 % — fail | 133,306 msg/s, 52 % — fail | BLE on 119,989 msg/s — fail; BLE off 0 msg/s but 14.5 % CPU — fail |
| C5 fencing from an external lease | n/a (none) | **pass** | fail | partial | fail |
| C6 per-database trim to an external index | n/a | **pass** | partial | pass | fail |
| C7 maintenance and maturity | — | ours | pass | fail | fail |
| C8 storage adaptation (lines; ranked) | — | 667 + node daemon | 422 + much | 442 + log engine | 608 + workarounds |

C3's absolute bound (50 µs per acknowledged write at concurrency 8) is failed by **every** candidate, including the
reference path that does the least work; at concurrency 64 the same code paths cost 27 (proto), 40 (custom), 39
(omnipaxos), 41 (openraft batched) and 65 (raft-rs) µs. On this in-process test bed the bound measures per-message
thread hand-offs more than the protocol. I report it as failed, as fixed, and do not move it.

## Decision (applying the rule fixed in advance)

1. **No library qualifies.** The rule requires C1, C2, C5 and C6 and no C4 failure. openraft fails C1, C2, C4 and C5;
   raft-rs is partial on C5 and fails C4 (and C7); omnipaxos fails C4, C5 and C6.
2. **Custom passes C1, C2, C4, C5, C6, C3's relative and memory bounds, and fails C3's absolute CPU bound** — as every
   candidate does, including the reference. By the letter of the rule custom is therefore *not cleanly qualified*: the
   rule says the report must say so and recommend the next step.
3. **Recommendation: custom**, on the published algorithm of the design (Raft log replication with lease-assigned
   epochs; Vertical-Paxos-style reconfiguration at upload points), implemented as `journal3z/core` already is (sans-IO,
   one state machine shared by the simulator, the exhaustive checker and the real-time path). Reasons: it is the only
   candidate whose fencing is the lease itself (C5) and whose idle cost does not grow with the number of databases
   (C4); its latency and throughput are the best measured; its trim is per database to the S3 marker (C6). **Next
   step for C3:** measure CPU per write on the real hardware with a real TCP transport in the next phase (soak plan),
   with the same 50 µs bound; the in-process test bed cannot answer it.
4. Closest library alternative, if the founder prefers not to own a protocol: raft-rs (git master), plus our own
   hibernation of idle groups, lease/term glue and the "lease holder behind" handling. That glue is most of the custom
   design's difficult part, without its single source of truth, and on a library with no release in 3.6 years.

Confidence: every number above is measured on this container (not on AWS); absolute latencies on AWS will differ
(walbench measured the proto path at p99 1.98 ms on real AWS vs 5.37 ms here: this test bed is more pessimistic). The
ranking between candidates is the result; the absolute numbers are not predictions.
