# Dictionary compression, local test (Fable idea 3)

Prompt: fenvix-brain `prompts/P-dictionary-compression-local.md`. Report: fenvix-brain
`sessions/2026-11-06-dict-compression-local/REPORT.md`. Spend $0, local only, the engine is not touched.

## What it measures

Four kinds of documents (kind 4 in two encodings), 5,000 training and 50,000 measured documents each (different
random samples):

1. `chat`: chat messages, the M29/M31 generator ported to real BSON (`src/synth.rs`).
2. `agent`: agent memory from real agent trajectories (tau2-bench public leaderboard, already in the repo since M25 on
   branch `agenti-bancone`): one message = one document with user, session, model, domain, role, turn, time, text,
   tool calls with JSON arguments, tool results (`src/agent.rs`). Day 1 = February/March 2026 runs (337,517 messages).
3. `orders`: orders with nested address, item lines and prices, the M29/M31 generator ported to BSON.
4. `agent_emb_bin` / `agent_emb_arr`: kind 2 plus two random 128-bit ids (UUID v4) and a 768-dimension unit-length
   embedding, as BSON binary float32 or as an array of doubles (9 significant digits, as APIs return JSON).

For each kind: zstd level 1 and 3, no dictionary and dictionaries trained on the 5,000 training documents (32, 64 and
112 KB), on (a) single records, (b) batches of 16 consecutive records in random order, (c) 64 KB blocks of records
sorted by customer and time; lz4 on (c). Compressed / raw BSON bytes, compress and decompress µs per record on one
core (median of 3). Frames: "lean" (magicless, no dictionary id, no checksum; the dictionary is named once in the
segment or box header) and "standard" (zstd's default frame without checksum) for (a) and (b).

Drift: the day-1 dictionaries applied to synthetic data 90 days later with new customers and a restarted app fleet,
to a new vocabulary, and to a schema with one added and one renamed field; for agent memory, to the August 2026 runs of
four newer models and to the same schema change. `agent_extras`: one dictionary per app (tau2 domain) and a 50,000
document training sample.

## Run

    ./fetch_traces.sh                    # extracts the M25 traces from branch agenti-bancone into ./traces
    cargo build --release
    taskset -c 2 ./target/release/dict-compression --reps 3 > results/run.log
    taskset -c 3 ./target/release/dict-compression --kinds agent_extras --reps 1 --out results/extras.jsonl > results/extras.log
    python3 -I summary.py                # results/summary.txt

About 6 minutes on one core of an Intel Xeon at 2.1 GHz. Results: `results/raw.jsonl`, `results/extras.jsonl`,
`results/run.log`, `results/summary.txt`.
