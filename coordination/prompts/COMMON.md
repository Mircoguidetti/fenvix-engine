# Common rules for owner and reviewer sessions (2026-11-06)

- Repo: you work only in fenvix-engine. The coordination files (working structure, component files, prompts) are in `coordination/` of fenvix-engine; the coordinator copies them to and from the brain repo. Do not attach other repositories.
- Brain documents named in prompts (DECISIONS.md, NOW.md, PLAN-LAVAGGIO.md, prompts/..., sessions/...) are copied under `coordination/brain/` with the same paths.
- Read first: `coordination/WORKING-STRUCTURE.md`, `coordination/components/README.md`, your component file. Brain documents cited by name (DECISIONS.md, reports in sessions/...) are summarised in your component file; if you need one in full, ask the coordinator.
- Everything you write is in English. The founder is Italian; you do not talk to him: you talk to the coordinator.
- The judge: in fenvix-engine run `judge/setup.sh` once, then `judge/run.sh` (phases: build unit diff-py diff-node ctl kill). It costs $0 and writes `judge/out/<commit>/VERDICT.md`. Commit only the VERDICT.md of your delivered commit. Long phases take a while: run them in the background.
- No AWS, no Atlas, no spending, no personal connectors, no merge to main, no deleting branches. The coordinator merges.
- Every number carries a label: MEASURED, ESTIMATED, DECLARED (with source) or UNKNOWN.
- The coordinator session is `session_01YAF6DhosFhuf9bLj4PrApk`. When you deliver, or when you are blocked, send it one message with send_message (tool of the claude-code-remote server; load it with ToolSearch if needed): "DELIVERED <repo> <branch> <commit>, judge: <one line per phase>" or "BLOCKED: <what, in one line>". If send_message is not available, write the same line at the top of your report and stop.
