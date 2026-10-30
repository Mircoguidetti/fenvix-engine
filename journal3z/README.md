# journal3z: the 3-zone journal (phase 1: design, simulator, spikes; no AWS)

Design: `../docs/journal3z-design.md`. Everything here is its own Cargo workspace (like `tools/walbench`): the engine's
workspace and `Cargo.lock` are not touched, and no engine code is changed.

| directory | what |
|---|---|
| `api/` | engine-facing `Journal` trait + `LocalJournal` (single node, round3 local-log semantics) + tests |
| `core/` | the protocol as sans-IO state machines (journal node, writer), shared by the simulator and the custom spike |
| `sim/` | deterministic simulator with fault injection, invariant checker, negative controls |
| `spikes/` | common test bed (delay line, fsync'd files) and one spike per candidate: proto, custom, openraft, raft-rs, omnipaxos |
| `tla/` | TLA+ model of the core protocol and TLC configurations |
| `tools/walbench-cycle1/` | replays the walbench quorum crash test draws (cycle-1 analysis) |
| `mc/` | exhaustive (bounded) model checker over the real `core` state machines |
| `results/` | raw results: `sim-*.json` (simulator campaign), `mc-*.json` (model checker), `spikes/*.json` (measurements) |

Reproduce: `sim/campaign.sh` (≈ 12 min, one core), `mc/run.sh`, `spikes/bench.sh` (≈ 10 min, needs an idle machine),
`cd api && cargo test`. Reports: `docs/journal3z-report.md` (start here), `docs/journal3z-design.md`,
`docs/journal3z-libraries.md`, `docs/journal3z-sim.md`, `docs/journal3z-tla.md`, `docs/journal3z-walbench-cycle1.md`,
`docs/journal3z-soak-plan.md`.
