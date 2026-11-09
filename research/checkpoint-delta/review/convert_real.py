"""Convert public agent trajectories into the thread scripts of research/checkpoint-delta/workload.py.

A script is a list of turns: {"user", "rounds": [{"thought", "calls": [{"id", "name", "args", "result"}]}],
"final", "ids"}. One turn = user message, r tool rounds (model asks, tools answer), final model answer;
LangGraph writes 3 + 2r checkpoints for it. The system prompt is dropped (a LangGraph app adds it at
model-call time; it is not in the state).

Datasets (all public, $0):
  tau_*      sierra-research/tau-bench historical_trajectories (git), OpenAI chat format, multi-turn.
  swe4       SWE-agent + Claude 4 Sonnet on SWE-bench Lite (s3://swe-bench-submissions, public),
             function calling, one user turn, many tool rounds.
  swe35      SWE-agent + Claude 3.5 Sonnet (2024), text actions: each model message is one round,
             the next observation is its result.

Usage: convert_real.py OUT_DIR   -> OUT_DIR/<dataset>.jsonl, one script per line
"""
import glob
import hashlib
import json
import os
import sys

DATA = os.path.expanduser("~/data")


def _text(c):
    if c is None:
        return ""
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        return "\n".join(x.get("text", "") if isinstance(x, dict) else str(x) for x in c)
    return str(c)


def _id(seed, k):
    return "msg_" + hashlib.blake2b(f"{seed}:{k}".encode(), digest_size=12).hexdigest()


def _args(a):
    if isinstance(a, dict):
        return a
    try:
        v = json.loads(a)
        return v if isinstance(v, dict) else {"input": v}
    except Exception:
        return {"input": a}


def from_chat(msgs, seed):
    """OpenAI-style chat (system/user/assistant with tool_calls/tool) -> turns."""
    msgs = [m for m in msgs if m.get("role") != "system"]
    turns, i, n = [], 0, len(msgs)
    while i < n:
        m = msgs[i]
        if m["role"] != "user":
            i += 1
            continue
        user = _text(m.get("content"))
        i += 1
        rounds, final, pending_thought = [], None, []
        while i < n and msgs[i]["role"] != "user":
            a = msgs[i]
            if a["role"] == "assistant" and a.get("tool_calls"):
                calls = []
                i += 1
                results = {}
                while i < n and msgs[i]["role"] == "tool":
                    results[msgs[i].get("tool_call_id")] = _text(msgs[i].get("content"))
                    i += 1
                for k, tc in enumerate(a["tool_calls"]):
                    tid = tc.get("id") or f"call_{seed}_{len(rounds)}_{k}"
                    fn = tc.get("function", tc)
                    calls.append({"id": tid, "name": fn.get("name", "tool"), "args": _args(fn.get("arguments", {})),
                                  "result": results.get(tid, "")})
                rounds.append({"thought": "\n".join(pending_thought + [_text(a.get("content"))]).strip(), "calls": calls})
                pending_thought = []
            elif a["role"] == "assistant":
                # a text answer: the turn's final if the user speaks next or the trajectory ends
                j = i + 1
                if j >= n or msgs[j]["role"] == "user":
                    final = "\n".join(pending_thought + [_text(a.get("content"))]).strip()
                    pending_thought = []
                else:
                    pending_thought.append(_text(a.get("content")))
                i += 1
            else:  # stray tool message
                i += 1
        if final is None:
            if not rounds:
                continue  # a trailing user message with no answer (e.g. ###STOP###): the app never ran
            final = "\n".join(pending_thought).strip()
        k = len(turns)
        turns.append({"user": user, "rounds": rounds, "final": final,
                      "ids": [_id(seed, f"{k}:{x}") for x in range(len(rounds) + 2)]})
    return turns


def tau():
    out = {}
    for f in sorted(glob.glob(f"{DATA}/tau-bench/historical_trajectories/*.json")):
        name = "tau_" + os.path.basename(f)[:-5].replace("-", "_").replace(".", "")
        ss = []
        for i, r in enumerate(json.load(open(f))):
            t = from_chat(r["traj"], f"{name}:{i}")
            if t:
                ss.append(t)
        out[name] = ss
    return out


def swe4():
    ss = []
    for f in sorted(glob.glob(f"{DATA}/swe/20250526_sweagent_claude-4-sonnet-20250514/*.traj")):
        d = json.load(open(f))
        h = d["history"]
        msgs = []
        for m in h:
            mm = {"role": m["role"], "content": m.get("content")}
            if m.get("tool_calls"):
                mm["tool_calls"] = m["tool_calls"]
            if m.get("tool_call_ids"):
                mm["tool_call_id"] = m["tool_call_ids"][0]
            msgs.append(mm)
        t = from_chat(msgs, os.path.basename(f))
        if t:
            sub = (d.get("info") or {}).get("submission") or ""
            if not t[-1]["final"]:
                t[-1]["final"] = sub[:20000] or "Done."
            ss.append(t)
    return {"swe4": ss}


def swe35():
    ss = []
    for f in sorted(glob.glob(f"{DATA}/swe/20240620_sweagent_claude3.5sonnet/*.traj")):
        d = json.load(open(f))
        h = [m for m in d["history"] if m["role"] != "system" and not m.get("is_demo")]
        seed = os.path.basename(f)
        user = _text(h[0]["content"])
        rounds, final = [], None
        i = 1
        while i < len(h):
            a = h[i]
            if a["role"] != "assistant":
                i += 1
                continue
            obs = _text(h[i + 1]["content"]) if i + 1 < len(h) and h[i + 1]["role"] == "user" else None
            action = a.get("action") or ""
            thought = a.get("thought") or ""
            if obs is None:
                final = _text(a["content"])
                i += 1
                continue
            rounds.append({"thought": thought, "calls": [{"id": f"call_{hashlib.blake2b(f'{seed}:{i}'.encode(), digest_size=12).hexdigest()}",
                                                          "name": "bash", "args": {"command": action}, "result": obs}]})
            i += 2
        final = final or ((d.get("info") or {}).get("submission") or "Done.")[:20000]
        ss.append([{"user": user, "rounds": rounds, "final": final,
                    "ids": [_id(seed, x) for x in range(len(rounds) + 2)]}])
    return {"swe35": ss}


def main():
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    allds = {**tau(), **swe4(), **swe35()}
    for name, ss in allds.items():
        with open(os.path.join(out, name + ".jsonl"), "w") as f:
            for s in ss:
                f.write(json.dumps(s) + "\n")
        cks = [sum(3 + 2 * len(t["rounds"]) for t in s) for s in ss]
        cks.sort()
        msgs = sum(sum(len(t["user"]) + len(t["final"]) + sum(len(c["result"]) + len(json.dumps(c["args"])) for r in t["rounds"] for c in r["calls"]) for t in s) for s in ss)
        print(f"{name}: {len(ss)} threads, checkpoints min {cks[0]} median {cks[len(cks)//2]} max {cks[-1]} mean {sum(cks)/len(cks):.0f}, "
              f"text {msgs/len(ss)/1000:.0f} KB/thread")


if __name__ == "__main__":
    main()
