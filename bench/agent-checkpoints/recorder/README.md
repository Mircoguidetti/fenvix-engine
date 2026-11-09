# Part C: record real agent traffic once, replay it against every database

RULES.md 2026-11-09: spend once, reuse many times. Real LangGraph agents with a real Claude model work on public-style
tasks; every checkpoint stream is recorded with its real timing; the recording is then replayed against our server,
Aurora, Supabase and Atlas with no further model call. All task data is generated here (no customer data).

- `tasks.py`: two task families. Support: a retail agent in the style of tau-bench's retail domain (generated store,
  scripted user turns, so no second model plays the user). Coding: a coding agent in the style of SWE-bench on small
  generated Python repositories with one bug and a failing test (tools run pytest in a temporary folder).
- `record.py`: a real compiled `StateGraph(MessagesState)` (agent and tools nodes). The model node calls the official
  Anthropic SDK (adaptive thinking, `output_config.effort` explicit, prompt caching, server-side refusal fallbacks
  `fallbacks: "default"`), or a fake model for $0 runs. `RecordingSaver` stores every `put` / `put_writes` exactly
  (LangGraph's serializer) with its time; every model call with its usage. Stops at `--max-dollars` and at the first
  API error. `--pilot` = 5 support + 5 coding conversations.
- `replay.py`: sends a recording to a target saver (`fenvix`, `mongo`, `postgres` from `../targets.py`), recorded order
  and timing (`--time-scale`), `--copies N` for volume, then resume / list / time travel per thread.
- `claude_cost.py`: the DECLARED prices (https://platform.claude.com/docs/en/about-claude/pricing, read 2026-11-09) and
  `estimate`, which turns a fake recording into tokens and dollars per conversation with prompt caching.

Commands:

    python record.py rec-fake --model fake --support 20 --coding 20          # $0, local
    python claude_cost.py estimate rec-fake --thinking-factor 2
    python replay.py rec-fake out-mongo --target mongo --uri mongodb://127.0.0.1:27017 --copies 2
    python record.py rec-pilot --model claude-sonnet-5-5 --pilot --max-dollars 5   # PAID: only after the gate of RULES.md

Model (coordinator's decision, 2026-11-09): Claude Sonnet 5.5, the typical agent model and half Opus 5.5's price.

Local validation (MEASURED, 2026-11-09, fake model, validation not results): 40 conversations recorded, 10.3 model
calls, 24.1 checkpoints and 0.18 MB of checkpoints per conversation (mean); replayed twice against local mongod 7.0:
1,924 puts p50 1.1 ms p99 3.1 ms, resume p50 1.8 ms, list p50 24 ms, time travel p50 1.8 ms.
