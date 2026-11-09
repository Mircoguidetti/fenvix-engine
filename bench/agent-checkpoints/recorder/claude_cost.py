"""Part C · Claude API prices and the cost estimate of the recording.
Prices DECLARED: https://platform.claude.com/docs/en/about-claude/pricing (read 2026-11-09 by the step9-prep session):
base input, 5-minute cache write (1.25x), cache hit, output, in dollars per million tokens; Batch API 50% off (not used:
an agent loop needs each answer before the next call). Haiku 5.5's prices are for prompts up to 100,000 tokens.
Usage:
  claude_cost.py estimate RECORDING_DIR [--thinking-factor 2]
      reads a FAKE recording (record.py --model fake), replays its calls as if prompt caching were on (each call reads
      the previous call's prompt from the cache and writes only the new part), and prints tokens and dollars per
      conversation for each model, plus the pilot (10 conversations) and the full run. ESTIMATED: tokens are characters
      / 4 of the fake replies, and thinking tokens are unknown: output is multiplied by --thinking-factor (2 = the
      visible output once more as thinking, an assumption to check in the pilot)."""
import glob
import gzip
import json
import sys

PRICES = {  # model: (input, cache write 5 min, cache hit, output) $ per MTok, DECLARED (URL above)
    "claude-opus-5-5": (4.00, 5.00, 0.20, 20.00),
    "claude-sonnet-5-5": (2.00, 2.50, 0.10, 10.00),
    "claude-haiku-5-5": (0.10, 0.125, 0.01, 0.50),
}


def call_dollars(model, u):
    i, w, r, o = PRICES[model]
    return (u.get("input_tokens", 0) * i + u.get("cache_creation_input_tokens", 0) * w
            + u.get("cache_read_input_tokens", 0) * r + u.get("output_tokens", 0) * o) / 1e6


def estimate(folder, thinking_factor=2.0):
    convs = []
    for f in sorted(glob.glob(f"{folder}/*.jsonl.gz")):
        calls = [json.loads(l)["usage"] for l in gzip.open(f, "rt") if '"model_call"' in l]
        prev, cached = 0, []
        for u in calls:
            p = u["input_tokens"]  # the whole prompt (fake: characters / 4)
            cached.append({"input_tokens": 0, "cache_read_input_tokens": prev, "cache_creation_input_tokens": max(0, p - prev),
                           "output_tokens": int(u["output_tokens"] * thinking_factor)})
            prev = p
        convs.append({"file": f.rsplit("/", 1)[1], "calls": cached})
    out = {}
    for m in PRICES:
        per = [sum(call_dollars(m, c) for c in cv["calls"]) for cv in convs]
        tokens = [sum(c["cache_read_input_tokens"] + c["cache_creation_input_tokens"] for c in cv["calls"]) for cv in convs]
        outs = [sum(c["output_tokens"] for c in cv["calls"]) for cv in convs]
        mean = sum(per) / len(per)
        out[m] = {"dollars_per_conversation": round(mean, 4), "prompt_tokens_per_conversation": sum(tokens) // len(tokens),
                  "output_tokens_per_conversation": sum(outs) // len(outs), "pilot_10": round(10 * mean, 2),
                  "full_200": round(200 * mean, 2), "full_400": round(400 * mean, 2)}
    return {"conversations": len(convs), "thinking_factor": thinking_factor, "models": out}


if __name__ == "__main__":
    if sys.argv[1] == "estimate":
        tf = float(sys.argv[sys.argv.index("--thinking-factor") + 1]) if "--thinking-factor" in sys.argv else 2.0
        print(json.dumps(estimate(sys.argv[2], tf), indent=1))
