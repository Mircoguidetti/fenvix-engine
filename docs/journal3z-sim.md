# journal3z: deterministic simulator, invariants, negative controls

Code: `journal3z/sim/` (simulator), `journal3z/core/` (the protocol state machines it runs — the same code the custom
spike runs in real time), `journal3z/mc/` (exhaustive checker, `docs/journal3z-tla.md`). Campaign script:
`journal3z/sim/campaign.sh`. Raw results: `journal3z/results/sim-*.json`. Everything below is **measured** (run in
this container on 2026-10-30) unless marked otherwise.

## 1. What is simulated

One process, one PRNG (xoshiro256**, seeded), one event queue ordered by (time, sequence): **a seed replays exactly**
(`journal3z-sim replay --seed N [--mutation M] [--trace]`). Simulated time in microseconds.

| component | model |
|---|---|
| 3 journal node slots, one per AZ | `NodeCore` from `journal3z/core`. Two states per node: `mem` (page cache, what the node answers from) and `dur` (what is on disk). A record is durable only when an fdatasync covering it completes (simulated latency 50–600 µs, slow-disk periods 20–200 ms). **A reply leaves the node only when every record the node produced before it is durable** (also for replies that produced no record themselves). |
| 3 writer servers, one per AZ | lease client (acquire / renew every TTL/3 / release; local clock with an offset), object-store open (writer fence + marker read), `WriterDb` from `journal3z/core`, client writes, periodic upload (every 100–800 ms) of everything appended so far (acknowledged or not), trim after the upload is confirmed, sleep, hot move, membership change at an upload point |
| S3 | lease objects with conditional writes (holder, expiry written in the holder's clock, seq, members) and database objects (writer fence epoch, marker, content). Each request 3–30 ms each way; outages. |
| network | per message 0.3–1.5 ms (1 % of messages +1–30 ms), drop 0.1 %, duplication 0.1 %, reordering by random delay; chaos periods (drop up to 20 %, duplication up to 10 %, extra delay up to 50 ms) |
| clients | one write every 2–20 ms per database, sent to a writer that believes it holds the lease |

Parameters (scaled down so that many takeovers happen): lease TTL 2 s, skew allowance 150 ms, clock offsets up to
±1.5 s (10× the allowance), 3 databases (10 in one campaign).

## 2. Faults (one every 50–400 ms in the main campaign)

kill -9 and restart of a node (10 ms–2 s, 20 % of them 3–15 s), **a stale node returning** (down 5–20 s), loss of a
node's disk (the replacement comes back with a **new identity**; at most one lost disk per database configuration at a
time, except in the double-failure campaign), kill -9 of a writer, **freeze of a writer** (10 ms–1 s or 2–6 s, longer
than the lease; half of them hit a writer while it has writes in flight; on resume it handles its queued network input
before or after its timers, at random), partitions (one entity isolated, a random two-sided split, **asymmetric**
one-way cuts, **a whole zone**: node + writer of an AZ cut from everything including S3, sometimes also crashed), message
drop / duplication / delay bursts, slow disk, **fdatasync failure** (the node exits; its unsynced records survive only
as a random prefix), **disk full** (every fdatasync fails while it lasts), **clock skew** (offset jumps up to ±1.5 s),
hot move of a database to another writer, S3 outage.

## 3. Invariants, checked after every event

| id | invariant | how |
|---|---|---|
| (a) | no acknowledged write is lost while at most one node's disk is lost | for each database: every acknowledged write above the S3 marker must be, at its index with its bytes, in the log that recovery rule R3 chooses from **every** pair of the database's current members whose disks exist, using their **durable** state only (as if all crashed now); every acknowledged write at or below the marker must be in S3 with its bytes |
| (a/recovery) | the writer whose barrier commits holds every acknowledged write below its barrier; an acknowledgement that arrives after a newer writer is active is in that writer's log | at the barrier commit and at every acknowledgement |
| (b1) | a node never accepts a FENCE or APPEND of an epoch lower than a promise it has already made known (a PROMISE or ACK it sent) | tracked by the checker per (node, database) from the replies actually sent |
| (b2) | one index, one acknowledged write | |
| (b3) | every write acknowledged by epoch e1 has an index below the barrier of every epoch e2 > e1 that committed its barrier | at every acknowledgement and every barrier commit |
| (c) | log matching on the bytes (same index and epoch anywhere → same bytes); a committed index never changes; a node acknowledges only entries it holds, and the committed entry when the writer is at least as new as the committer | at every record and every ACK |
| (d) | a node trims only below the S3 marker; S3 agrees with every committed entry | at every trim record and every upload |

**Liveness** (reported, not an invariant): at 80 % of each run every fault is repaired at once and no new fault is
injected; every database must acknowledge writes again; the time to the first acknowledgement after the repair is
reported.

## 4. Results

| campaign | seeds | steps (events) | violations | acknowledged writes | takeovers | membership changes | late acks of a resumed writer | fenced requests | liveness after repair (p50 / max) |
|---|---|---|---|---|---|---|---|---|---|
| main (fault every 50–400 ms) | 1–1000 | **200,000,000** | **0** | 4,324,116 | 101,560 | 12,813 | 658 | 9,066 | 166 ms / 3.9 s, 3000/3000 databases resumed |
| moderate (fault every 1–3 s) | 1001–1500 | 100,000,000 | 0 | 7,469,201 | 5,425 | 1,188 | 43 | 6,711 | 5 ms / 3.2 s, all resumed |
| 10 databases | 2001–2200 | 40,000,000 | 0 | 1,241,803 | 27,441 | 4,206 | 156 | 1,840 | 161 ms / 5.1 s, all resumed |
| calm (no fault) | 3001–3100 | 20,000,000 | 0 | 1,488,374 | 0 | 0 | 0 | 0 | — (97 % of client writes accepted) |
| **total** | 1,800 seeds | **360,000,000** | **0** | 14,523,494 | | | | | |

Fault counts in the main campaign: partitions 97,646 (asymmetric 24,453, zone outages 24,646), clock skew 65,577,
hot moves 65,164, network chaos 65,788, writer kill -9 62,906, writer freezes 51,314, fsync errors 49,487, slow disks
49,134, node kill -9 45,457, disk full 32,841, S3 outages 32,781, stale nodes returning 18,059, node disks lost 4,261.
Wall time 227 s for 200 M steps (one core).

Availability is not a goal of the main campaign (its fault rate keeps two of three nodes, or S3, unavailable much of
the time): 9.6 % of client write attempts found a writer in the main campaign, 83 % in the moderate one, 97 % with no
faults. What matters is that it never traded safety for it, and that every database resumed once the faults stopped.

### 4.1 Double failures (expected to lose data)

`--double` allows a second disk loss before the first is repaired (seeds 4001–4200, 40 M steps). The durability
invariant is not asserted where 2 of a database's 3 member disks are gone; instead the simulator counts steps at which
an acknowledged write exists nowhere but on lost disks: **42,764,640 of the 45,919,391 such steps** had lost
acknowledged writes, and no database resumed after the repair (its configuration still names 2 dead nodes: operator
action needed). This is the failure matrix's "two disks lost → acknowledged writes since the last upload may be lost",
observed, and it shows the checker does see loss when loss happens.

### 4.2 Negative controls: every deliberate bug is caught

`journal3z-sim controls --seeds 1-100 --steps 200000`: for each bug, seeds are run until the first violation. Each is
run twice: with every invariant, and with **only the durability invariants (a)** — so that a control is not caught only
by a check written for it.

| deliberate bug | caught (all invariants): invariant, seeds, steps | caught by durability (a) alone |
|---|---|---|
| `no_fencing`: nodes accept any epoch | b1, 2 seeds, 310,964 steps | yes, 2 seeds, 310,973 steps |
| `ack_on_one`: acknowledge with 1 of 3 | a, 1 seed, 268 steps | yes, 268 steps |
| `ack_gap` (wrong catch-up): a node acks a range it does not hold | c, 154 steps | yes, 164 steps |
| `keep_diverged` (wrong catch-up): a node keeps a diverged entry and acks | c, 80,913 steps | yes, 36 seeds, 7,020,145 steps |
| `recovery_longest` (wrong recovery): longest log, epochs ignored | a, 58,308 steps | yes, 58,308 steps |
| `recovery_single` (wrong recovery): one promise instead of 2 | c, 26,480 steps | yes, 26,481 steps |
| `trim_before_upload` | d, 2,110 steps | yes, 2,114 steps |
| `ack_before_fsync` | a, 582 steps | yes, 582 steps |
| `ignore_fsync_error` | a, 1,780 steps | yes, 1,780 steps |
| `skip_store_fence`: the new writer does not fence the old S3 writer | d, 19 seeds, 3,756,793 steps | yes, 3,756,822 steps |
| `wipe_same_id`: a node with a lost disk returns under its old identity | a, 18,058 steps | yes, 18,058 steps |
| `forget_fence`: a node drops its fence when its log becomes empty | b1, 6 seeds, 1,116,142 steps | **not within 100 seeds × 200,000 steps**; the loss is shown deterministically below |

`forget_fence` needs a stale writer to resume after another writer has uploaded and trimmed every node empty, and to
still believe its lease valid; the random runs reach the fence violation (b1) but not the loss itself. The scenario
`journal3z-sim scenario forget_fence` builds exactly that interleaving: with the fence record kept, the resumed writer
is told it is stale and nothing is acknowledged; with the bug, its write at index 6 is acknowledged by 2 nodes while S3
(marker 9) holds another write at index 6 — an acknowledged write that exists nowhere
(`results/sim-scenario-forget-fence.jsonl`).

### 4.3 Deterministic scenario: walbench cycle 1

`journal3z-sim scenario walbench_cycle1`: a writer acknowledges 200 writes while node 0 is down (nodes 1+2); recovery
starts while node 0 is still down. walbench's fixed-pair reader (pair 0,2) fails with "connection refused"; the
journal3z open fences and reads any 2 of 3 and recovers 200/200, 0 lost.

## 5. Problems found and fixed during this phase (all traced, none in the protocol rules)

1. **Checker too strict (3 cases, 5 false violations in 200 seeds).** (i) Recovery completeness checked when a writer
   becomes *active*: a writer that finished its open from promises collected *before* a newer epoch fenced the nodes is
   locally active but can never commit; checking at its barrier commit is the right property. (ii) The same check
   counted acknowledgements *above* the barrier, which belong to newer epochs (b3 covers them). (iii) "A node's
   acknowledged range agrees with every committed entry" must only apply to writers at least as new as the committer:
   a lagging node may legitimately accept a stale writer's entry at an index committed later by a newer epoch (Raft
   Figure 8 (e)). Each violation was replayed from its seed, the trace read, and the check corrected; the same
   correction was made to the design document's invariant (c). The negative controls were re-run after the change
   (table above).
2. **A path the faults did not exercise.** In the first version, a frozen writer handled its timer before its queued
   network input on resume, so it always abandoned its lease first and the "late acknowledgement after a takeover"
   path never ran (0 times in 1,000 seeds). Freezes now hit writers with writes in flight half of the time, and the
   resume order is random: the path ran 658 times in the main campaign.
3. **Negative control mis-modelled.** `forget_fence` first dropped the trim point too, which produced a livelock
   instead of the intended bug; it now drops only the fence epoch.

## 6. What the simulator does not cover

- The engine itself (SlateDB, the memtable, the marker written inside the database's batches) is modelled as "S3
  content up to the marker". Not verified against the engine.
- Byte-level disk faults (bit flips, misdirected writes, torn sectors inside a record) are not injected; the model is
  "a prefix of the unsynced records survives". The local backend's CRC per frame is tested separately (torn tail).
- Real timing (latency numbers) — see `docs/journal3z-libraries.md`.
- Erase (GDPR) is not exercised by the simulator (it is in the local backend's tests).
- One database configuration of exactly 3 nodes; no node pool larger than 3.
