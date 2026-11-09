FIX 2

# Review round 2: compaction 2 step 0, the TLA+ model (`c2-step0-tla`, f2e6c05)

Reviewer session, 2026-11-09. This is the same session that wrote round 1, asked by the coordinator to check only the round 1 findings, the takeover note and the owner's new finding. Spend: $0, local only, no AWS, nothing merged. Owner's report: fenvix-brain `sessions/2026-11-08-c2-step0-fix/REPORT.md`.

Branch `review-c2-step0-r2` (from f2e6c05). What I added is in `research/compaction-2/tla/review-r2/`:
- `Compaction2R2.tla`: a copy of the fixed spec, plus two behaviours it leaves out (constant `Rv`) and their remedies (`RvFix`).
- `MCR2.tla`, `gen_review_r2.py`, `run_review_r2.sh`, `trace.py`.
- `output/`: every TLC output, with every trace.

Tool: TLC 1.8.0 (the same jar as both earlier runs), Java 21, 4 workers.

## 1. In one line

All five round 1 findings and the takeover note are fixed in the main spec. Each remedy has a control that fails when the remedy is removed, and I re-ran all nine (MEASURED). The new rules are right as far as they go. Two cases next to F1 and N1 are still open, each with a failing configuration against the fixed spec's own rules (MEASURED):
- A PUT that has landed but whose answer has not reached the waiter yet. This needs a rule that ties the PUT timeout to the lease.
- A cut-off old waiter that starts new reads after the takeover. This needs a rule on when the old waiter stops serving.

Both remedies are one sentence each, and both pass in TLC (MEASURED).

## 2. Round 1 findings: checked

The review copy with `Rv = "none"` is the fixed spec: on the owner's `ZombieShortLease-ok` configuration it finds exactly 2,458,607 distinct states, complete, depth 25, the owner's count (MEASURED, `output/none-ZombieShortLease-ok.txt`).

I re-ran the nine new controls on f2e6c05. Each fails on the invariant the owner reports (MEASURED):
- `NoFence-crash`: `NoBadRead`.
- `NoFence-timeout`: `NoBadRead`.
- `FollowerOldEenv`: `NoBadRead`.
- `E1NoWait`: `NoBadRead`.
- `FollowerNoMarker`: `NoBadRead`.
- `EraseOneGroup`: `LatestReadOK`.
- `CkNoRecheck`: `CkptReadOK`.
- `RollbackPlain`: `EncodingOK`.
- `ZombieShortLease`: `NoBadRead`.

The outputs are in `output/controls-rerun/`. Distinct-state counts at the stop differ slightly from the owner's (for example 69,749 against 70,666 for `NoFence-crash`), because TLC with 4 workers stops at whichever worker finds the violation first. I took the owner's 17 earlier controls and the twins from the owner's outputs, which I read with `summarize.py`.

What I checked in the spec, finding by finding:
- **F1 and the owner's timeout case.**
  - What the spec does now:
    - Holds pin `known`, the version the waiter's process knows, and the compactor builds on it.
    - A crash leaves the old PUT unheard, and `PublishTimeout` lets a live waiter lose the answer.
    - `Fence` publishes a copy at slot `known + 1` before any hold or publish. A late PUT targets the same slot, so it fails.
  - Rules 17, 24 and 25 say this correctly.
  - The owner's timeout case is right, and the same fence covers it.
  - Still open: the answer of a PUT that did not time out can arrive late. See F6 below.
- **F2.** E1 is announced first, and writing starts only once the announcement is at least T_pin old. The marker is read after the step's view is open, which is the right order. A follower that refreshed before the announcement cannot read past T_pin, so it never sees an envelope. Rules 6 and 26 are correct.
- **F3.** `InG` covers reads, delta merges (an erasure stays while it is above any of its groups' X_g) and reseals. Rules 13, 14 and 22 are correct. The main shape still keeps one customer per group; the spanning layout runs only in `EraseOneGroup-ok` (complete) and `Span-ok` (budget). That is enough for a paper check.
- **F4.** Rule 18 now has the lease life, the check after SlateDB's checkpoint exists, and the clock-skew margin. It is correct.
- **F5.** Rule 21 is now step 2's rule. It is correct.
- **N1.** For one hold life after the takeover, the new waiter's drops use the remote lease. That is correct for holds that were already open. Rules 27 and 28 do not cover holds the old waiter opens after the takeover. See F7 below.
- **N5, N6.** Rules 5 and 17 are correct.

## 3. The two open cases (each a configuration that FAILS with the fixed spec's rules, `Ctl = "none"`)

### F6. A PUT lands, and its answer reaches the waiter later: a read misses a committed row (`MCR2-AckDelay.cfg`)

What the model leaves out: `PublishLand` lands the version and tells the waiter in one step. In the code the PUT completes in the store first, and its answer arrives later. Network delays and SDK retries can stretch that up to the PUT timeout. Until the answer arrives, the waiter is not "unsure" (nothing has timed out), so it does not fence, and rule 24 has its holds pin the old version.

TLC's trace (MEASURED, `NoBadRead` violated at depth 11, 26,896 states, no crash, no timeout):
1. put a1, flush, absorb.
2. The PUT of v2 lands, and its answer is still on the way.
3. A drop job sees v2. Two ticks pass, one ordinary lease.
4. The waiter's own drop job removes the put. Its floor is X_v2 and no hold is open, which is correct by rules 7 and 8.
5. A find begins on v1, the version the waiter still knows, and reads a1 as absent.

The same gap breaks the remote floor (rule 10) and GC grace (rule 23), which counts from the version's landing.

Remedy (`MCR2-AckDelay-fix.cfg`): a PUT whose answer has not arrived within a timeout shorter than the lease counts as timed out, so the fence follows (rule 25). In TLC this is a bound on time between the PUT's start and its answer. With a 1-tick timeout: PASS within a 15-minute budget, 30,178,623 distinct states, depth 20, no violation (MEASURED, `output/AckDelay-fix.txt`). With a 2-tick timeout, which equals the lease, it fails at depth 11 (MEASURED, `output/AckDelay-t2.txt`).

Rule for the code:
- The PUT timeout, retries included, must be shorter than the lease minus a margin. For example, under 30 s with the proposed 60 s lease.
- The remote lease of rule 10 and the GC grace of rule 23 must each exceed the hold life plus that timeout.

### F7. The cut-off old waiter begins new holds after the takeover (`MCR2-ZombieBegin.cfg`)

What the model leaves out: after a cut-off takeover, the old waiter's open holds keep reading (N1), but it starts no new hold. A cut-off waiter that still reaches its clients and S3 can keep accepting reads until it notices it was fenced. Reads never write, so they never meet the fence.

TLC's trace (MEASURED, `NoBadRead` violated at depth 14):
1. An erasure at seq 1 and a put of a1 at seq 2, flush, absorb.
2. A takeover of the cut-off waiter, while its PUT of v2 is in flight. The old waiter knows v1 at seq 2.
3. The late PUT lands.
4. One tick later, the old waiter begins a new find on v1.
5. Two more ticks pass, which ends rule 27's window of one hold life after the takeover. The new waiter's drop then uses the ordinary lease, and removes the put.
6. The old waiter's find, two ticks old, reads a1 as absent.

Remedy (`MCR2-ZombieBegin-fix.cfg`):
- The old waiter begins no hold once its serving lease (the waiter lease of guarantee 8) has expired.
- Rule 27's window counts from that expiry: one serving lease plus one hold life after the takeover.

With a 1-tick serving lease: PASS within a 15-minute budget, 24,545,809 distinct states, depth 17 (the violation is at depth 14), `output/ZombieBegin-fix.txt`, no violation (MEASURED). This also needs the waiter lease to work as a read lease: the new waiter takes over only after the old one's lease has expired, and the old one stops starting reads by then. Whether today's waiter lease does that is UNKNOWN to me.

## 4. Notes (they do not block)

- The main shape still keeps one customer per group, and it is complete only up to 11 steps. The deep cases live in reduced configurations. The owner says so in section 5 of the report.
- The fence is atomic in the model. Rule 25 says correctly that a fence that times out is followed by another fence. With F6's timeout rule, that also bounds how long holds wait.
- `CkptNoLease-ok` no longer closes (88.8 million states within budget). The new `CkNoRecheck-ok` closes, so the checkpoint path still has one complete run.
- The owner did not run the judge, because the round changes no engine code. I confirmed that: `git diff a888e09 f2e6c05` touches only `research/compaction-2/tla/` (MEASURED). I did not run it either.

## 5. Rules to add (for the coordinator, into `RULES-FOR-THE-CODE.md` and the step 3 and 4 prompts)

- To rules 8, 10, 23 and 25: a PUT timeout, retries included, shorter than the lease minus a margin. The remote lease and the GC grace each exceed the hold life plus that timeout (F6).
- To rules 27 and 28: the old waiter begins no hold after its serving lease expired, and the new waiter's remote-lease window lasts one serving lease plus one hold life (F7). Step 4's takeover test should open a new transaction on the old waiter after the takeover.
