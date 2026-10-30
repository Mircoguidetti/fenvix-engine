# journal3z: formal model and exhaustive checking

Two artefacts, with different status:

| artefact | what | status |
|---|---|---|
| `journal3z/tla/Journal3Z.tla` (+ `MC.tla`, `MC.cfg`) | TLA+ specification of the core protocol for one database: 3 nodes, writers identified by epoch, object store with writer fence and marker, upload + trim, a message set (late, duplicated, reordered or lost delivery). Invariants: `Durability` (a), `NoTwoAcks` (b2), `OrderedEpochs` (b3), `LogMatching` (c1), `TrimSafe` + `S3Agrees` (d). Constants for 4 negative controls (`NoFence`, `AckOnOne`, `Longest`, `TrimEarly`). | **Written, NOT model-checked.** Running the TLA+ tools (`tla2tools.jar`, downloaded from the tlaplus GitHub release) was not permitted in this session's sandbox (external code). Not verified — it may contain mistakes that TLC would reveal. |
| `journal3z/mc` | an exhaustive explicit-state checker written in Rust that explores **the real protocol code** (`journal3z-core`: `NodeCore`, `WriterDb`) for one database, checking the same invariants on every reachable state | **Run** (results below) |

## How to run TLC (for the founder or the next phase)

```bash
cd journal3z/tla
curl -sSLO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -workers auto -deadlock MC
```

Expected: no invariant violated with the 4 control constants `FALSE` (`MC.cfg`); a counterexample for each control set
to `TRUE`. Bounds in `MC.cfg`: 3 nodes, `MaxEpoch = 2`, `MaxIdx = 2` (indices include the barrier).

## The Rust exhaustive checker (`journal3z/mc`)

State: the 3 nodes (durable state: a node replies after fdatasync, so handling a message and persisting it is one
step), every writer's `WriterDb`, the object store (writer fence epoch, marker, content), the pending messages, the set
of acknowledged writes. Actions: a writer opens (store fence + marker read + FENCE to every node; writers open in
increasing epoch order because the store fence refuses older ones), any pending message is delivered (in any order;
a message may also never be delivered = loss), a client write, an upload of any prefix by the store's current writer
followed by TRIM messages, the Reading-phase retransmission. Every reachable state is checked against (a) durability by
rule R3 from every 2 of 3 nodes or S3, (b2), (b3), (c1), (d).

Two message models: **bag** (default: any pending message can be delivered next, i.e. arbitrary reordering; a
delivered message is consumed) and **FIFO** (`--fifo`: one queue per writer–node link, as the real design's one TCP
connection per pair; the head of any link is delivered or lost, so reordering across links and loss remain).
Abstractions (what this check does not cover; the random simulator does): duplication (`--dup` keeps messages for tiny
bounds), node crashes with loss of unsynced data, disk loss and
reconfiguration, lease clocks (epochs simply open in increasing order, and an old writer keeps running after a newer
one opened — the frozen/stale writer case), retransmission ticks in the Active phase (`--ticks`).

Search: depth-first, visited set of 64-bit state hashes (a hash collision could hide a state; with n states the
probability is about n²/2⁶⁵, below 10⁻⁴ for n = 40 M). A run that hits the state cap is reported as **bounded, not
exhaustive**.

## Results (`journal3z/results/mc-*.json`, reproduce with `journal3z/mc/run.sh`)

Bounds: 3 nodes, one database, `idx` = highest index a writer may create (the barrier takes one), so `idx 2` = each
writer's barrier plus one client write when the indices allow it.

| writers (epochs) | idx | messages | mutation | distinct states | verdict | time |
|---|---|---|---|---|---|---|
| 1 | 1 | bag + duplication | none | 21,021 | **complete, no violation** | 0.9 s |
| 1 | 2 | bag | none | 18,698,439 | **complete, no violation** | 515 s |
| 1 | 2 | FIFO | none | 2,765,555 | **complete, no violation** | 50 s |
| 2 | 2 | bag | none | 40,000,000 (cap) | bounded, no violation in the explored part | 1,214 s |
| 2 | 2 | bag | `ack_on_one` | 100,700 | **caught** (a), depth 16 | 2.5 s |
| 2 | 2 | bag | `ack_gap` | 336,061 | **caught** (a), depth 20 | 8.4 s |
| 2 | 2 | bag | `trim_before_upload` | 533,722 | **caught** (d), depth 9 | 16.2 s |
| 2 | 2 | bag | `no_fencing` | 20,000,000 (cap) | **not caught within the cap** | 568 s |
| 2 | 2 | bag | `recovery_longest` | 20,000,000 (cap) | not caught within the cap | 559 s |
| 2 | 2 | bag | `recovery_single` | 20,000,000 (cap) | not caught within the cap | 580 s |
| 2 | 2 | bag | `keep_diverged` | 20,000,000 (cap) | not caught within the cap | 537 s |
| 2 | 2 | FIFO | none and `no_fencing` | runs to 100 M states stopped by me after 31 min (≈ 25 M states each, still running); no result recorded | not completed | |
| 2 | 2 | FIFO, randomized order, seeds 1–4 | `no_fencing` | 1,500,000 each (cap) | not caught | ≈ 55 s each |

**What this means.** With one writer the protocol is checked exhaustively at these bounds (every interleaving of
FENCE/PROMISE/READ/APPEND/ACK/MISMATCH/TRIM deliveries, losses and uploads): no reachable state violates (a), (b2),
(b3), (c1) or (d). With two writers the space is larger than the caps. The bounded searches caught the three bugs a
single writer can trigger quickly, but **not** the four that need a stale and a new writer to interleave in a specific
way. That `no_fencing` counterexample does exist within these bounds: it is written out as a deterministic schedule in
`journal3z/core` (`schedule_tests::no_fencing_loses_an_acknowledged_write`: with the fence, the stale writer's write is
never acknowledged; without it, it is acknowledged and then overwritten by the new writer's barrier). So for two
writers the depth-first search with a state cap is **weak evidence**, and the safety claim for interleaved writers
rests on the random simulator (where all four are caught within 1–36 seeds) and on the published algorithm, not on
this checker. Randomized search order (bag: 3 seeds × 2 M states for `no_fencing`, `recovery_longest`, `recovery_single`; FIFO: 4 seeds × 1.5 M states for `no_fencing`) did not find them either.

Next step (not done): model-check `Journal3Z.tla` with TLC (symmetry over nodes, a state-space constraint), or add
partial-order reduction to `journal3z/mc` (deliveries to different nodes commute).
