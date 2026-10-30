# The walbench quorum "connection refused" cycle: harness, not protocol

Question: in the walbench run 41017093239 (eu-west-1), the quorum crash test reported 200 / 200 cycles, 0 lost, and
**1 harness failure**: `recover failed: Error: node 0: connect ... Connection refused (os error 111)`. Was it a harness
problem or a protocol/recovery problem?

**Answer: a harness sequencing problem.** The crash test's recovery tried to read node 0 while node 0 was still inside
the 15 s restart delay left by the scenario that ran just before it. No acknowledged write was involved and no data was
missing; the recovery process could not connect, so the cycle was not judged. The recovery design flaw it exposes
(reading a *fixed* pair instead of *any* 2 of 3) is fixed by construction in journal3z (design D6).

## Evidence

1. **Which cycle and which pair.** `results-walbench/raw/results/crash-quorum.json`: cycle 1 (log `crquorum-c1`,
   variant w1, conc 8, 200 B, kill_ms 836) has the harness error; the record does not contain its read pair, because
   the error path drops it. The draws are reproducible: `crashtest` uses `StdRng::seed_from_u64(seed)` and the agent
   passed seed 5 (`tools/walbench/instance/agent.py`, `crash_args("quorum", 5)`). `journal3z/tools/walbench-cycle1`
   replays the draws with the same `rand` 0.8.8 and the same order of calls (variant, conc, size, kill_ms, kill_acks,
   pair). Result (`cargo run --release | python3 check.py results-walbench/raw/results/crash-quorum.json`):

   ```
   failed cycle 1: replayed read pair = 0,2; error = 'recover failed: Error: node 0: connect\n\nCaused by:\n    Connection refused (os error 111)'
   cycles compared: 200, all recorded fields equal: 200, different: 0
   cycles whose recovery read node 0: 133 (failed: 1)
   first successful cycle that read node 0: 3
   ```

   All 200 cycles match on every recorded field (variant, conc, size, kill_ms, kill_acks and, where recorded, the read
   pair), so the replayed pair of cycle 1 is the pair the harness used: **0,2**.

2. **Node 0 was down at that moment, by design of the previous step.** `quorumtest` (`tools/walbench/bench/src/quorumtest.rs`)
   sends node 0 a `CRASH(15000 ms)` at t = 45.004 s (`results-walbench/raw/results/quorumtest.json`, event
   `node0 SIGKILL`). The node supervisor (`tools/walbench/instance/node-supervisor.sh`) restarts the node only after
   sleeping those 15 s. Clock alignment: node 1's supervisor log prints `07:01:49 node exited` for the SIGKILL at
   t = 15.003 s, so t = 0 is 07:01:34 (±1 s, the log has 1 s resolution), node 0 died at ≈ 07:02:19 and could listen
   again at ≈ 07:02:34–07:02:35 at the earliest.

3. **The crash test started before that.** `results-walbench/raw/logs/client-agent.log`:
   `07:02:33Z step quorumtest: exit 0 in 59 s` and `07:02:33Z step crash-quorum: start`. The agent runs the steps
   back to back (`agent.py` lines 315–317) and nothing waits for node 0. Cycle 0 read pair 1,2 (did not touch node 0)
   and finished in well under 2 s (kill after 266 acks); cycle 1's recovery therefore ran at ≈ 07:02:34–07:02:36,
   inside node 0's restart window. "Connection refused" (not a timeout) is what a reachable host with no listener on
   the port returns: the client instance was up, the node process was not.

4. **Node 0 was fine afterwards.** Cycle 3 (pair 0,2) and 132 later cycles read node 0 successfully; in total 133
   cycles read node 0 and only cycle 1 failed.

5. **No loss hidden in the failed cycle.** The writer of cycle 1 had nodes 1 and 2 (2 of 3) and could acknowledge;
   whether it lost anything is unknown because the recovery did not run, so the cycle is "not judged", not "0 lost".
   The run's statement "0 lost in 199 judged cycles + 1 not judged" is the accurate one.

Not available: node 0's own log (it was on the client instance in the agent's work directory and was not collected
into `results-walbench/raw/logs/`), which would have shown the restart time directly.

## Is it a protocol problem? Can the simulator reproduce it?

The walbench recovery (`QuorumStore::read_all`) reads a fixed list of nodes and fails if any of them does not answer.
That is a **liveness** defect of the harness's reader, not a durability defect: with 2 of 3 acknowledgements, any 2
nodes hold every acknowledged write, so reading the two nodes that *are* up would have succeeded. In journal3z the
open/recovery reads **any** 2 of the 3 members that answer (design §5.2 step 3, D6). The simulator covers the
situation (a node down during recovery, every seed) and `journal3z/sim` has a deterministic scenario
`walbench_cycle1`: node 0 down, recovery started with the fixed-pair reader (walbench behaviour) → unavailable, no loss;
with the journal3z reader → recovery succeeds from nodes 1+2 (`docs/journal3z-sim.md`).

Fix for any future walbench-style run: before a crash test, wait until every node answers (the agent should poll
the nodes after `quorumtest`), and make the recovery read any 2 of 3.
