# Draft runs: method and gates (written before measuring)

Prompt: fenvix-brain `prompts/P-draft-runs-traces.md`. Idea A of `sessions/2026-11-07-fable-long-term-market/REPORT.md`.
Spend $0. Raw traces stay outside git (default `/home/user/data`).

## Sources

- tau2-bench paper results (`data/tau2/results/final/*_{default,base}_*4trials.json`, the files M25 used).
- tau2-bench 2026 leaderboard (the 27 compact extracts M25 downloaded, branch `agenti-bancone`,
  `proto/agenti-bancone/tracce/tau2-classifica/`).
- tau-bench original (`historical_trajectories`, no timestamps).
- DAB (M18 raw traces, branch `maitre-agenti`).
- SWE-bench Verified, mini-swe-agent with Gemini 3.5 Flash (timestamps per command).
- Human traces for collisions: Ubuntu IRC (11 channels, 2012-01-02..2012-04-29), LSApp, CDNOW
  (same files and md5 as `research/return-prediction`).

## Definitions for agent runs

- Run = one simulation / trajectory (each trial is its own run, own customer).
- Only calls made by the agent count; tools run on the end user's own phone (tau2 telecom user tools) are not the
  database.
- Write = a successful call of a tool whose type is WRITE in the tau2 source (call_discoverable_agent_tool is
  unwrapped to the tool it calls). GENERIC tools (transfer to human, calculate) are not writes.
- Writing run = a run with at least one write.
- Documents: the tau2 tables are the collections, a record is a document. Which documents each write tool changes
  is read from the tool source (`effects.py`, one rule per tool, with the line of code it comes from); the ids come
  from the call arguments and the initial database. Inserts create a new document.
- Customer of a document: the owner from M25's generic `Owners` (users / customers table, `user_id` /
  `customer_id` fields, ids referenced by a single owner). A document with no owner is shared (flights, products,
  plans, knowledge base).
- Customers written = distinct owners of the written documents. A write to a shared document counts as touching
  "shared" (reported apart: it is a second unit for the per-customer draft).
- Read-your-own-writes (RYW): a successful read, later in the run than a write, whose documents include a document
  written earlier in the same run (document level). Reads are mapped to documents the same way (by id argument; a
  query by user reads that user's documents of the table it queries). A looser customer-level variant (any read of
  a customer after a write to that customer) is reported as an upper bound.
- Multi-document run: the run's writes touch two or more distinct documents. Reported two ways: across separate
  write calls (only a draft or an app-side buffer can make them atomic) and inside one call (a normal short
  transaction already does it).
- Wall-clock length: last message time minus first message time (tau2, SWE). Also reported: the span from the
  first read of the written customer to the last write (what a normal MongoDB transaction would have to cover),
  and the span with every gap between messages capped at 120 s (to remove benchmark waits).
- No timestamps (tau-bench, DAB): agent steps and user turns drawn from the distributions MEASURED on the tau2 paper
  (M25 `StepModel`); also the deterministic count x median step.
- SWE: customer = the repository; write = a shell command that changes a file (sed -i, redirection to a file,
  tee, patch/git apply, cp/mv/rm, python writing a file); RYW = a later command that reads an edited path or runs
  code/tests after an edit.

## Collisions on human traces

- A customer = one user (IRC nick over all channels, LSApp user, CDNOW customer). Every event is a write by or
  about that customer (an IRC message, an app event, a purchase).
- Two anchors, both reported:
  1. Uniform moment: for every (customer, day) with at least one event, a moment drawn uniformly in that day;
     P(another event of the customer in (t, t+L]).
  2. Event anchor: the run starts at one of the customer's own events (the agent reacts to the customer);
     P(the customer's next event falls in (t, t+L]).
- IRC timestamps are whole minutes and CDNOW whole days: events are spread uniformly inside their minute/day
  (ESTIMATED for CDNOW, where L is far below the resolution).
- L = median and p90 of the writing-run lengths found in point 1.
- Customer level is an upper bound on a real conflict (a conflict needs the other write to touch the draft's read
  or write set).

## Gates (from the prompt, not changed)

1. CONTINUE if >= 30% of writing runs exceed 60 s and >= 80% write a single customer. DROP if < 10% exceed 60 s,
   or > 40% write several customers. Else MIXED.
2. CONTINUE if >= 30% of writing runs have RYW or a multi-document write; DROP if < 10%. Else MIXED.
3. CONTINUE if the collision probability at the median length on chat (IRC) is < 10%; DROP if > 30%. Else MIXED.
