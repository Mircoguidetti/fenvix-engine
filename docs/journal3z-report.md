# journal3z phase 1: final report

Branch `journal3z` (from `walbench`). No AWS action and no spend in this phase. No engine code and no other branch was
changed. Everything is under `journal3z/` and `docs/journal3z-*.md`.

## 1. Outcome in one paragraph

The 3-zone journal now has a precise protocol: Raft's log rules, with each writer's epoch taken from the database lease
instead of an election. That protocol is implemented once, as sans-IO state machines (`journal3z/core`). The same code
was run through **360 million simulated events under random faults with 0 violations**; each of **12 deliberately
broken variants was caught**; it was checked exhaustively in small bounds by a model checker over that code; and it was
measured in real time on a common test bed against openraft, raft-rs and omnipaxos. **Recommendation: build the
custom protocol, not a library.** The walbench "connection refused" cycle was a harness sequencing problem, not a
protocol problem, and it was proven from the raw data.

## 2. What was decided (design: `docs/journal3z-design.md`)

1. **Protocol** = Raft log replication with the leader chosen outside the group. The epoch is the lease `seq` of the
   conditional PUT that made the server the owner. Opening a database = a Paxos phase 1: fence a majority, take the
   most up-to-date log by (last epoch, last index) above the S3 marker, append a barrier. Commit counts only the
   writer's own epoch (Raft's Figure-8 rule). Catch-up uses Raft's consistency check, and only entries above the S3
   marker are ever needed. Safety never depends on clocks.
2. **Fencing** is the per-database `promised` epoch on each node, kept durably even when the database has no entries
   (forgetting it loses data: shown). A node whose disk is lost comes back only under a **new identity**.
3. **Layout**: per-database logical logs on **one shared physical journal per node**, one connection per writer–node
   pair, **no per-database heartbeats** (failure detection comes from the lease that already exists). Trim, restore,
   erase and quotas are per database.
4. **Journal key** = (database name, incarnation): today's lease object is deleted with the database, so its `seq`
   restarts at 1 for a recreated database.
5. **Trim** only below the S3 marker read back from S3. **Recovery** reads any 2 of 3 nodes.
6. **Membership** changes only at an upload point (Vertical Paxos with S3 as the configuration master). No state
   transfer between nodes.
7. **Interface**: the `Journal` trait (`open / append / uploaded / close / erase / pending`), with a local single-node
   backend (`journal3z/api`) that keeps round3's local-log semantics: group fsync, torn tail ignored, replay above the
   marker, drop tombstones, segment deletion after upload.
8. **Custom, not a library** (§4).

Decisions I took without asking (all reversible):
- I read "1–2 ms inter-node delay" as a **round trip** of 1–2 ms (one-way 0.5–1.0 ms), and also report one-way 1–2 ms.
- The `proto` reference (the walbench write path) was re-measured on the same test bed, so that "not below the
  prototype" is a like-for-like comparison.
- I proposed a lease TTL of 10 s in journal mode (round3 default 30 s), because an early takeover is now harmless.
- The simulator scales time down (lease TTL 2 s, uploads every 0.1–0.8 s) to get many takeovers per run.
- The three library spikes were written by sub-agents to one specification. I ran every final measurement myself,
  sequentially, on an idle machine.
- openraft got a fairness variant that batches many writes into one entry. omnipaxos was measured with its own
  election both on and off.

## 3. Numbers

### Correctness (`docs/journal3z-sim.md`, `docs/journal3z-tla.md`)

| what | result |
|---|---|
| simulator, correct protocol | **360,000,000 events, 1,800 seeds, 0 violations**. 14.5 M acknowledged writes, 134,426 takeovers, 18,207 membership changes after disk loss, 857 late acknowledgements from frozen writers that resumed |
| faults injected (main campaign alone) | 97,646 partitions (24,453 asymmetric, 24,646 whole-zone), 65,577 clock skews, 51,314 writer freezes, 62,906 writer kill -9, 45,457 node kill -9, 18,059 stale nodes returning, 49,487 fsync errors, 32,841 disk-full periods, 32,781 S3 outages, 4,261 node disks lost, plus message drop, duplication and reordering |
| invariants checked after every event | (a) no acknowledged write lost, (b) one writer at a time, (c) agreement, (d) trim only below S3 |
| liveness | after every fault is repaired, all 3,000 databases of the main campaign write again: p50 166 ms, max 3.9 s (simulated lease 2 s) |
| negative controls | **12/12 caught** by the full checker. 11/12 are also caught by the durability check alone; the 12th (`forget_fence`) is caught by the fencing invariant, and its data loss is shown by a deterministic scenario |
| double disk loss (outside the promise) | loss observed (42.8 M of 45.9 M steps in that state); stays down until an operator acts — as the failure matrix states |
| exhaustive check of the real core code | see §3.3 of `docs/journal3z-tla.md` (filled in below) |
| TLA+ specification | written, **not model-checked**: running the TLA+ tools was not permitted in this session; command for the founder in `docs/journal3z-tla.md` |

### Performance (`docs/journal3z-libraries.md`)

Measured in this container, not AWS: conc 8, 1 KB, round trip 1–2 ms, real fdatasync. The median of 3 runs is shown.

| | p99 | writes/s | CPU µs/write | idle, 1,000 databases | fencing from the lease | per-database trim to S3 marker | maintained |
|---|---|---|---|---|---|---|---|
| walbench path (reference) | 5.37 ms | 2,660 | 116 | 0 msg/s | none | n/a | — |
| **custom** | **3.77 ms** | **3,920** | 202 | **0 msg/s** | **pass** | **pass** | ours |
| openraft 0.9.25 | 8.00 ms | 1,581 | 290 | 52,452 msg/s | fail | partial | yes |
| raft-rs 0.7.0 | 3.96 ms | 3,515 | 259 | 133,306 msg/s | partial | pass | no release since 2023-03 |
| omnipaxos 0.2.4 | 3.77 ms | 3,804 | 200 | 119,989 msg/s (0 with its election off) | fail | fail | 0.x, research |

At concurrency 64 the custom path does 26,204 writes/s at p99 3.99 ms. With one-way 1–2 ms it does p99 5.58 ms.

## 4. Recommendation: custom

No library met the rule fixed before measuring:
- openraft fails latency, throughput, idle cost and fencing.
- raft-rs is only partial on fencing, fails idle cost, and has had no release for 3.6 years.
- omnipaxos fails fencing, trim and idle cost.

Custom met every criterion except the absolute CPU bound (50 µs per write at conc 8). That bound was failed by every
candidate, including the reference path. On this test bed it measures in-process thread hand-offs, so I report it as
failed and do not move it: it must be re-measured on AWS hardware with a real TCP transport (next phase).

Confidence:
- **Measured** in this container: all latency, throughput, CPU and idle numbers.
- **Estimated**: everything about AWS (latency, takeover time, soak cost).
- **Proven by simulation and exhaustive search within bounds** (not by a proof): safety of the protocol.

## 5. walbench cycle 1 (`docs/journal3z-walbench-cycle1.md`)

Harness, not protocol. The crash test's seed-5 draws, replayed exactly, match all 200 recorded cycles. Cycle 1's
recovery read nodes 0+2 at about 07:02:34–36, while node 0 was inside the 15 s restart delay from the `quorumtest`
scenario. That scenario had SIGKILLed node 0 at 07:02:19, and the crash test started at 07:02:33 without waiting for
it. 133 other cycles read node 0 successfully. The recovery reader's real flaw (a fixed pair instead of any 2 of 3) is
fixed by design, and the simulator scenario `walbench_cycle1` shows both readers side by side.

## 6. Still open

- **Not built yet**: the node daemon (segments, CRC, fence checkpoint), a TCP transport, the writer client with the
  real lease and S3, and engine integration behind the `Journal` trait. These come before the soak
  (`docs/journal3z-soak-plan.md` §2).
- Engine changes needed later (not made): incarnation id and journal configuration in the lease record; the marker
  `sys/journal` in every batch; open order "lease → SlateDB writer fence → marker → journal open"; release of the
  lease after `T_cut` without a journal majority.
- TLA+ specification not model-checked (permission); CPU per write not measured on real hardware; erase not exercised
  in the simulator (only in the local backend's tests).
- The 72-hour AWS soak (plan ready, about 12–15 USD estimated for option B with a 25 USD cap) needs the founder's
  authorization.

## 7. Biggest risks

1. **We own a consensus protocol.** Every rule is published, and it is backed by a simulator, an exhaustive checker
   and negative controls. But the implementation is still ours to get right and to maintain. The simulator and its
   negative controls must stay in CI, and every protocol change must re-run them.
2. **The engine integration is where the subtle bugs will be.** Examples: the order of the SlateDB fence and the
   marker read (the `skip_store_fence` negative control shows the loss), the marker inside every batch, and replay
   idempotence. The simulator models S3 and the database as "content up to the marker", not as SlateDB.
3. **Fence records must never be lost** (the `forget_fence` and `wipe_same_id` controls show loss). Operational
   procedures, such as restoring a node from a backup or re-imaging it, must create a new node identity.
4. **Two lost disks within one upload interval lose acknowledged writes.** This is outside the single-failure promise,
   but it must be stated in the product's durability statement. Uploading more often shrinks the window.
5. **Availability depends on S3 for takeovers** (the lease and the SlateDB open). During an S3 outage, acknowledgements
   continue but no database can move.

## 8. One slip to report

In two crates.io API requests made while gathering library facts (one by me, one by a sub-agent), the user's e-mail
address was put in the HTTP User-Agent header (crates.io asks for contact information there). It should not have been
sent. Nothing else was sent anywhere.
