# Draft runs (Fable long-term idea A): trace test

Prompt: fenvix-brain `prompts/P-draft-runs-traces.md`. Report: fenvix-brain
`sessions/2026-11-07-draft-runs-traces/REPORT.md`. Method and gates, written before measuring: `METHOD.md`.
Spend $0; nothing here touches the engine.

## Files

- `docs_model.py`: which documents each tau2 tool call reads and writes (rules read from the tau2 source), owners.
- `agent_runs.py`: loaders (tau2 paper and 2026 leaderboard, tau-bench, DAB, SWE-bench) and per-run metrics.
- `run_agents.py`: points 1 and 2 -> `results/agents.json`, `results/agent_runs.tsv.gz` (one line per run).
- `collisions.py`: point 3 on IRC, LSApp, CDNOW -> `results/collisions.json`.
- `summary.py`: gates and point 4 -> `results/summary.txt`.
- `test_draft_runs.py`: 10 sanity tests (`python3 test_draft_runs.py`).

## Data (outside git, default /home/user/data, override with DR_DATA)

    git clone --depth 1 https://github.com/sierra-research/tau2-bench $DR_DATA/tau2        # commit of 2026-11-04
    git clone --depth 1 https://github.com/sierra-research/tau-bench  $DR_DATA/taub
    git clone --depth 1 https://github.com/john-b-yang/20260901_mini-v2.4.2_gemini-3-5-flash $DR_DATA/swem
    # tau2 2026 leaderboard extracts (M25): branch agenti-bancone, proto/agenti-bancone/tracce/tau2-classifica/*.jsonl.gz
    #   -> $DR_DATA/tau2-board/
    # DAB raw traces (M18): branch maitre-agenti, proto/maitre-agenti/tracce/dab-grezze (cat parts | tar xJ) -> $DR_DATA/dabraw
    # IRC: research/return-prediction/fetch_irc.sh $DR_DATA/irc 2012-01-02 2012-04-29 <11 channels>
    # LSApp: github.com/aliannejadi/lsapp, lsapp.tsv (md5 67f4e7fda2c3edcdcebe0245b333c3fa) -> $DR_DATA/lsapp/
    # CDNOW: CDNOW_master.txt from the Lifetimes 0.11.3 sdist (md5 37ee2da5127d4a369d6fe0052678624d),
    #   extracted (not installed) under $DR_DATA/cdnow_pkg/

## Run (under a minute)

    python3 run_agents.py
    python3 collisions.py 60 78.6 131.8 301.5 600 1200 1240.4 1501.4 1800 3600
    python3 summary.py
