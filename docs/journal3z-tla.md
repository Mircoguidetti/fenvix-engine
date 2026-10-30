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

Abstractions (what this check does not cover; the random simulator does): a delivered message is consumed (no
duplication; `--dup` keeps messages for tiny bounds), node crashes with loss of unsynced data, disk loss and
reconfiguration, lease clocks (epochs simply open in increasing order, and an old writer keeps running after a newer
one opened — the frozen/stale writer case), retransmission ticks in the Active phase (`--ticks`).

Search: depth-first, visited set of 64-bit state hashes (a hash collision could hide a state; with n states the
probability is about n²/2⁶⁵, below 10⁻⁴ for n = 40 M). A run that hits the state cap is reported as **bounded, not
exhaustive**.

## Results

(filled in from `journal3z/results/mc-*.json`)
