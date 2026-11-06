APPROVED: both FIX items resolved. Cross-zone saving -20% (1,000 writes/s) / -24% (5,000) MEASURED, -41% / -44% ESTIMATED; keeper-led failover 1.12 s median, max 1.88 s, 0 acks lost (MEASURED, with the fix round's 0.5-1 s election timeouts; 2.07 s median, max 2.21 s with the delivered 1-2 s). KEEP is NOT settled: +0.30 ms on my sandbox, +1.63 ms on the owner's (MEASURED); my round-1 "KEEP stands" is withdrawn

# Review of log-on-pair, round 2 (fix round 5602bf5..fc800ea; code and results 2ed5163, verdict fc800ea)

Reviewer branch: `review-log-on-pair-r2` (based on fc800ea). Scope as asked: only my two round-1 FIX items, the owner's
KEEP warning, and the fast judge. One 4-core sandbox, $0. The owner's report is in fenvix-brain, which I did not attach
(my instructions: fenvix-engine only), so I checked the figures as the coordinator relayed them against the committed
data and my own runs. For the same reason I did not write the copy to fenvix-brain `sessions/.../REPORT.md`: this
file is the report, to be copied by someone with access.

## FIX 1 (cross-zone): resolved

The relayed wording, -20% / -24% MEASURED and -41% ESTIMATED, is what `proto/quaderno/review/crosszone_today.py`
gives. The fix round does not change what crosses zones: on the fix binary the pair still measures 2.32-2.33 copies
per write, the same as base-near (`pair_analyze.py results/log-on-pair/fix-contract-rerun/phases.jsonl`). The fifth
number in round 1, "0% for a leader-near customer", should travel with it.

## FIX 2 (failover when the keeper must lead): resolved, with one condition on how the number is quoted

The code fix (`node.rs`): the keeper campaigns one election timeout after the old leader's last message when a
behind data copy asked for its vote, even if it still counted that leader as heard recently. It hands back by a
leadership transfer: it stops taking records, waits until the target has its whole log on disk, sends `TimeoutNow`,
and steps down. Safety: `TimeoutNow` only skips the target's prevote; its real election still needs a majority under
the usual up-to-date rule, and the keeper refuses commits while the transfer is under way. Nothing here weakens the ack
or vote rules.

My re-run of my own command, `pair_kill9.py OUT --cycles 24 --seed 7 --spare-down-ms 1500` on 2ed5163
(`results/log-on-pair/review2-kill9-spare-down-1500ms/`), MEASURED:

* 0 of 242,400 acknowledged writes lost or misplaced on any copy; 20,225 probe writes to the old waiter, all refused
  (NotLeader); no panics.
* The keeper led in **24 of 24** cycles and handed back by transfer in 24 of 24 (its log; the status poll saw it lead
  in 2). No transfer timed out.
* Write stop: **1.12 s median, max 1.88 s** (owner: 1.13 / 1.63; same 24 cycles, so the max is run-to-run noise).

**The second stop at the hand-back.** The owner is right that my round-1 metric (kill to first ack) missed it.
`pair_stops.py` only counts later gaps above 500 ms, so I checked below that, subtracting the same gap sum over a quiet
window before each kill (the writer acknowledges in bursts): `proto/quaderno/review/stops_vs_baseline.py`.
On the fix: excess later stop **-0.02 s median** at a 150 ms threshold, -0.004 s at 250 ms. The transfer hands back
with no visible stop. On the round-1 code it was +0.49 s / +0.67 s median, so the old total was about 4 s, not 3.05 s
(`pair_stops.py` there: 3.96 s total median).

**The condition.** The fix round also halved the harness's election timeout, from 1-2 s to 0.5-1 s (`pair_common.py`),
and the 1.13 s needs it. With the delivered timeouts (`PAIR_ELECT_MIN_MS=1000 PAIR_ELECT_MAX_MS=2000`, same command,
`results/log-on-pair/review2-kill9-spare-down-1500ms-old-timeouts/`): **1.99 s first stop, 2.07 s total median, max
2.21 s**, 0 of 258,000 lost (owner: 2.08 s). So the code fix takes the keeper path from about 4.0 s to about 2.1 s, and the
timeout change takes it to 1.1 s. The shorter timeout would speed up today's shape just as much, so it is not a gain of
the pair. Quote 1.13 s only with "0.5-1 s election timeouts". The commit message and the committed old-timeout run say
this, so it is disclosed. In my 20 latency phases on the fix binary (0.5-1 s) no copy other than the first leader ever
campaigned (the owner reports the same over 46 phases). That setting is untested at 8,000 reads/s and with cold reads,
where cores 0-1 run at 70-83%.

## The KEEP warning: confirmed on the owner's sandbox, not on mine

My fix-binary re-run, 5 interleaved reps, warm page cache (`results/log-on-pair/review2-contract-rerun-warm/`):
base-near 3.71 ms, pair-2000 3.94 ms, **+0.23 ms**. With my round-1 reps (10+10): **+0.30 ms, 90% interval
+0.02..+0.61** (`results/log-on-pair/review2-contract-stats.txt`). The owner's fix-round sandbox (18+18):
**+1.63 ms, interval +1.24..+1.97**. Pooling every phase from both sandboxes gives +1.23 ms (NEITHER), but that mixes
two baselines (base-near 3.7-3.9 ms on mine, 4.8 ms on the owner's), so I report the two sandboxes separately. Both runs
have 0 disk reads in the pair phases, and the same code (the delivered binary also gave +1.24 ms on the owner's
sandbox), so the gap between them is the machine. The verdict depends on the sandbox it runs on, and the sandbox cannot
decide it. It needs the contract case on the real instance types (m8gd.large class for the waiter). My round-1 line
"KEEP stands narrowly" is withdrawn.

A first attempt right after a container restart, with a cold page cache (`results/log-on-pair/review2-contract-rerun-cold/`),
broke: mongo-server finds took 500+ ms, 2 of 5 pair phases failed (the read generator timed out), and one phase had an
ack **p99 of 1,213 ms**. No election happened; copy 1 was starved by a mongo-server reading a cold disk on its cores.
That is information for contract point 2 (reads that hit the disk), not a valid contract phase. It also shows that
`pair_analyze.py` does not flag such a phase as invalid (it passed its checks).

## Fast judge on fc800ea

Two fast runs on fc800ea. Both: build PASS; unit FAIL 101/1 (`counter::tests::rev_maps_leave_room_for_active_data`);
diff-py and diff-node FAIL 412/413 (`bucketauto.infinity`); kill PASS. These are identical to integration's 01d64c6.
**ctl FAILed once in each run, on a different file each time**:
* run 1, `test_01_databases::test_registry_survives_restart`: the restarted server exited with "Address already in use"
  (a port-reuse race on restart). 5 of 5 passes when re-run alone.
* run 2 (the committed `judge/out/fc800ea/VERDICT-FAST.md`), `test_05_erase::test_erase_refused_while_forks_read_it_then_allowed`:
  one usage record (`control/usage/par/<date>/erase-...json`) was still in the bucket after the erase. 3 of 3 passes
  when re-run alone.

The only change outside `proto/` since 01d64c6 is the new `loadgen/src/bin/pairload.rs`, so this branch cannot cause
them. They are flaky engine tests in this sandbox, worth an issue for integration; they do not count against log-on-pair.
The owner's "identical to integration" holds except for this flakiness.

## NOTES

1. FIX 1's wording could not be read in the report itself (fenvix-brain not attached); only the relayed figures were checked.
2. The keeper path is now 0.3 s slower than the spare-current path at the same timeouts (1.12 s vs 0.84 s median over all cycles, owner's
   `fix-kill9-spare-current`), which is reasonable.
3. Round-1 content follows unchanged below.

---

FIX 2: KEEP stands (pooled p99 +0.79 ms, 12+12 reps, 90% interval +0.44..+1.14 ms, MEASURED); cross-zone saving MEASURED is -20% (1,000 writes/s) / -24% (5,000), the -41% / -45% needs an unmeasured spare feed (ESTIMATED); failover when the keeper must lead: writes stop 3.05 s median, max 4.96 s (MEASURED), not 1.5-1.9 s

# Review of log-on-pair, commits 8185868..465f93b (code 1118801, results 8d125b8, verdict 465f93b)

Reviewer branch: `review-log-on-pair` (based on 465f93b). Everything below was run in one 4-core sandbox like the owner's
(cgroup v1, one disk `vda`, `pq dproxy` 0.5 ms one way). No AWS, no money spent.

## Headline numbers after review

| claim | owner | after review | label |
|---|---|---|---|
| (1) ack p99, pair-2000 minus base-near, 1,000 writes/s | +0.96 ms (3 reps); 7 reps +0.97 | my 5 reps: **+0.36 ms**; pooled 12+12: **+0.79 ms**, 90% interval +0.44..+1.14, P(<1 ms) 0.84 → **KEEP, narrowly** | MEASURED |
| (3) acknowledged writes lost, 96 + 24 cycles | 0 of 966,200 | **0 of 293,500** in 24 more cycles (18 kill -9, 6 freeze), the keeper leading and handing over in every one | MEASURED |
| (3) old waiter acknowledgements | 0 | **0** (18,644 probe writes, all refused NotLeader) | MEASURED |
| (3) write unavailability per failover | median 1.86 s | spare current: **1.50 / 1.52 s** (writer's ack log, owner's two after-fix runs); keeper must lead: **3.05 s, max 4.96 s** | MEASURED |
| (4) cross-zone copies per write | 2.33 vs ~3.9 today (-41%); busy -45% | pair 2.33 = today leader-near 2.33 (**0%**); vs 1/3 near + 2/3 far: **-20%** (2.33 vs 2.91), busy **-24%** (2.11 vs 2.77); -41% / -44% only if today's spare also gets a cross-zone feed of +1.0 copy, which nothing measured | MEASURED / ESTIMATED |
| (5) CPU of copy 1 | 0.34 core at 1,000 w/s, 0.50 at 5,000, ~40 µs marginal | reproduces from the owner's data; my sandbox: 430-446 µs/record at 1,000 w/s (0.43-0.45 core) | MEASURED (machine-dependent) |
| fast judge on 465f93b | (verdict was on 1118801) | identical to integration's 01d64c6 | MEASURED |

## FIX 1: the cross-zone saving is ESTIMATED, not MEASURED

Script: `proto/quaderno/review/crosszone_today.py results/log-on-pair/run1-main/phases.jsonl` (output in
`results/log-on-pair/review-crosszone.txt`). Why it matters: the claim "2.33 vs about 3.9 today (-41%)" is labelled
MEASURED, but no phase measures 3.9. The script reproduces it exactly only as
`1/3 × base-near (2.33) + 2/3 × base-far (3.20) + 1.0` = 3.91 (busy: 2.11/3 + 2 × 3.10/3 + 1.0 = 3.77 → -44%). The +1.0
is a cross-zone feed of the warm spare in today's shape that was never measured. If today's spare tails the log
replica in its own zone (today's cell has one per zone), that term is 0. MEASURED: pair 2.33 vs today 2.91 (-20%) at
1,000 writes/s, 2.11 vs 2.77 (-24%) at 5,000. For a leader-near customer the pair saves nothing (2.33 vs 2.33). The
byte counting itself is right: every replica-to-replica route both ways, plus client routes to non-local copies,
divided by one copy's bytes (r0->r1).

## FIX 2: the failover path where the keeper must lead was not tested, and it is twice as slow

Script: `proto/quaderno/scripts/pair_kill9.py OUT --cycles 24 --seed 7 --spare-down-ms 1500` (new option). Results in
`results/log-on-pair/review-kill9-spare-down-1500ms/` (result.json, cycles.jsonl, acked.csv.gz, election lines,
unavailability.txt from `proto/quaderno/review/unavailability.py`). Why it matters: the dangerous schedule is "an
acknowledged write is on copy 1 and the keeper only, then the waiter dies". In the owner's runs the spare and the
keeper receive at the same speed on one machine, so the spare had every acknowledged write at every kill (gap 0 in all
32 cycles of the final run). On the delivered code the keeper led **0 times** in the final run (its log has no
"guida" line) and the other after-fix run committed no keeper log; it led 19 times only in the pre-fix run, where lost
vote requests delayed the data copies' elections. Freezing the spare
does not help: the 200,000-entry in-flight window and the proxy's unbounded queue deliver everything on wake (I tried
it: the keeper never led). The new option kill -9s the SPARE 1.5 s before each kill/freeze of the waiter, then restarts
it, so ~1,500 acknowledged records are on copy 1 and the keeper only.

Result (MEASURED, 24 cycles): no acknowledged write lost or misplaced on any copy (293,500), the spare has all of them
after catch-up, the old waiter acknowledged 0 probe writes, no panics. The keeper led in all 24 cycles and handed over
in all 24 (24 "guida" and 24 "keeper hands over" lines in its log; the status poll caught it leading in 12). Safety holds on this path. But writes stopped for **3.05 s median, max 4.96 s**
(writer's own ack log), against 1.50 s when the spare is current. The keeper took its "campaign now" shortcut
(`node.rs` `on_vote`) in only 12 of the 24 cycles; it needs `!heard_recently`, and the restarted spare's prevote can
arrive while the keeper still counts the dead waiter as heard recently. In the other 12 the keeper waited out its full
`PQ_KEEPER_WAIT_MS` (3,000 ms), which matches the ~3 s stops.
The headline must carry both numbers. A spare that just restarted (deploy, crash) when the waiter dies is the ordinary
case for this path.

## Checked and found right (no change)

* **Ack rule** (`maybe_commit`): `min(own durable, max(remote matched))` is never weaker than plain 2 of 3; followers
  ack only `durable` (fdatasync) entries; `term_at(n) == term` kept. The keeper changes alter only election timing
  (when to campaign, step down after commit), not vote rules, so Raft safety is unchanged. Split brain: the old waiter,
  restarted or woken, answered NotLeader to every probe in all 24 + 96 cycles.
* **Latency baseline**: same writer (`pq corsie`, open loop, 0 dropped), same machine, writer reaches r0 directly in
  both base-near and pair, same peer routes through the same proxy, r0 on cores 0-1 in both. Cases are interleaved
  within every repetition. Measuring from the scheduled instant instead of the send gives the same delta (+0.93 vs
  +0.97 ms on the owner's 7 reps), so "from the send" hides nothing.
* **dproxy**: identical on every peer route in both cases; the extra load of the pair case (the read generator on
  cores 2-3 with the proxy, copies 2 and 3) can only add jitter to the pair, not to the baseline.
* **CPU counting**: utime+stime of the r0 process over the window divided by acknowledged records; 0.34 / 0.50 core and
  ~40 µs marginal follow from the committed phases.

## NOTES (do not change the verdict)

1. KEEP is narrow. Owner's 3 reps +0.96, owner's 4 extra reps alone +1.21 (NEITHER), owner's run3 (copies 2 and 3 in
   RAM, which removes a handicap of the pair) +1.08 (NEITHER), my 5 reps +0.36. Pooled: +0.79 ms, P(<1 ms) 0.84
   (`proto/quaderno/review/contract_stats.py`, output in `results/log-on-pair/review-contract-rerun/contract_stats.txt`).
   My run is a little easier: the 10% uniform reads were mostly served from the OS page cache left by the fill (0 disk
   reads/s vs 249/s in the owner's run1). A real-hardware run should repeat the contract case before anyone builds on it.
2. The 1.86 s failover median cannot be reproduced from the committed data: status polling gives 1.79 / 1.47 s, the
   writer's ack log 1.50 / 1.52 s (kill -9 cycles). It is pessimistic, so harmless.
3. 32 of the 96 kill cycles (374,200 of the 966,200 acks) ran before the transport fix, on code that is not the
   delivered one. On the delivered code: 64 cycles, 592,000 acks, plus my 24 cycles, 293,500 acks.
4. kill -9 on one machine cannot test fsync (the page cache survives) nor the loss of the waiter's machine with copy 1
   on its local disk. An ack can be "copy 1 + keeper": after losing the waiter's machine those writes live on the keeper
   alone until the spare catches up. That is the 2-of-3 bargain, but worth saying to the founder.
5. The keeper steps down when a data copy has `commit`, not its last index; with writes arriving during its turn the
   data copy can still be behind the keeper's log, and the keeper must lead again (terms jumped by 2 in several of my
   cycles). Liveness cost only.
6. Copy 1's CPU per record is machine-dependent: 342-371 µs/record (owner) vs 430-446 µs/record (mine) at 1,000 writes/s
   on the same sandbox type.
7. The verdict on the branch (`judge/out/1118801`) was on the code commit, not 465f93b. I ran it on 465f93b:
   build PASS; unit FAIL 101/1 (`counter::tests::rev_maps_leave_room_for_active_data`); diff-py and diff-node FAIL
   412/413 (`bucketauto.infinity`); ctl PASS (3 files); kill PASS. Identical to integration's 01d64c6; the only
   change outside `proto/` since 01d64c6 is the new `loadgen/src/bin/pairload.rs`. The judge's kill phase rewrites
   `loadtest/results/gc/durability-s3-durable.json`; I reverted that.
8. The owner's report and pre-registered plan are not in this repository (they live in fenvix-brain), so I checked
   the claims as given in my instructions against the committed data.
