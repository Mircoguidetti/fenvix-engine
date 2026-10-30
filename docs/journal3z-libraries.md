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

(filled in after the spikes ran; see below)
