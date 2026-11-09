"""Deterministic agent workload: text corpus, thread scripts, and the LangGraph graph.

A thread is a list of user turns. Each turn is: user message, then `r` tool rounds
(model asks for 1-3 tools, ToolNode answers), then a final model answer.
With LangGraph 1.2.x one `invoke` of a turn writes 3 + 2r checkpoints
(input, start, model, then tools + model per round), so a script can hit
an exact checkpoint count N. `check_counts` in run_savers.py verifies it.

Sizes (bytes of text, lognormal, ESTIMATED, see README "Message sizes"):
  user message      median 250,  sigma 0.9, 20..4,000
  final answer      median 900,  sigma 0.8, 50..6,000
  tool-call thought empty 60%, else median 150
  tool arguments    median 120 (JSON)
  tool result       median 1,800, sigma 1.1, 80..40,000 (code 50%, search JSON 30%, prose 20%)
Tool rounds per turn: 0: 25%, 1: 35%, 2: 20%, 3: 10%, 4-6: 10%. Calls per round: 1: 70%, 2: 20%, 3: 10%.
"""
import ast
import json
import math
import os
import random
from functools import lru_cache
from typing import Annotated, Any

from langchain_core.tools import InjectedToolCallId, StructuredTool

SEED = 20261109
TOOLS = ["search_web", "read_file", "run_sql", "fetch_url", "list_dir"]
MODEL = "gpt-4.1-2025-04-14"


@lru_cache(maxsize=1)
def corpus() -> tuple[str, str]:
    """(prose, code) from the Python standard library: real English docstrings and real code.
    Sorted file list, so the corpus is the same on every run."""
    root = os.path.dirname(os.__file__)
    files = sorted(
        os.path.join(d, f)
        for d, _, fs in os.walk(root)
        if "site-packages" not in d and "test" not in d
        for f in fs
        if f.endswith(".py")
    )
    prose, code = [], []
    for p in files:
        try:
            src = open(p, encoding="utf-8").read()
            tree = ast.parse(src)
        except Exception:
            continue
        code.append(src)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module, ast.AsyncFunctionDef)):
                ds = ast.get_docstring(node)
                if ds and len(ds) > 80:
                    prose.append(" ".join(ds.split()))
        if sum(map(len, code)) > 40_000_000:
            break
    return "\n\n".join(prose), "\n".join(code)


def _lognorm(rng: random.Random, median: float, sigma: float, lo: int, hi: int) -> int:
    return int(min(hi, max(lo, math.exp(math.log(median) + sigma * rng.gauss(0, 1)))))


def _slice(rng: random.Random, text: str, n: int) -> str:
    start = rng.randrange(0, len(text) - n - 1)
    return text[start : start + n]


def _tool_result(rng: random.Random, tool: str) -> str:
    prose, code = corpus()
    n = _lognorm(rng, 1800, 1.1, 80, 40_000)
    kind = rng.random()
    if kind < 0.5:
        return _slice(rng, code, n)
    if kind < 0.8:
        hits = []
        while sum(len(h["snippet"]) for h in hits) < n:
            s = _slice(rng, prose, min(400, max(60, n // 4)))
            hits.append(
                {
                    "title": s[:60].strip(),
                    "url": f"https://docs.example.org/{rng.randrange(10**6)}/{rng.randrange(10**4)}",
                    "snippet": s,
                    "score": round(rng.random(), 4),
                }
            )
        return json.dumps({"results": hits})
    return _slice(rng, prose, n)


def thread_script(n_checkpoints: int, rng: random.Random) -> list[dict]:
    """Turns whose checkpoint counts (3 + 2r each) sum exactly to n_checkpoints."""
    prose, _ = corpus()

    def feasible(rem: int) -> bool:
        return rem == 0 or rem == 3 or rem >= 5

    turns, rem, t = [], n_checkpoints, 0
    while rem > 0:
        w = rng.random()
        r = 0 if w < 0.25 else 1 if w < 0.60 else 2 if w < 0.80 else 3 if w < 0.90 else rng.randint(4, 6)
        r = min(r, (rem - 3) // 2)
        while r > 0 and not feasible(rem - 3 - 2 * r):
            r -= 1
        if not feasible(rem - 3 - 2 * r):
            # rem - 3 is 1, 2 or 4: take the whole remainder in one turn when odd, else split
            r = (rem - 3) // 2 if rem % 2 == 1 else (rem - 6) // 2
        rounds = []
        for k in range(r):
            c = rng.random()
            ncalls = 1 if c < 0.7 else 2 if c < 0.9 else 3
            thought = "" if rng.random() < 0.6 else _slice(rng, prose, _lognorm(rng, 150, 0.7, 20, 1200))
            calls = []
            for j in range(ncalls):
                tool = rng.choice(TOOLS)
                arg = _slice(rng, prose, _lognorm(rng, 100, 0.6, 10, 800))
                calls.append(
                    {
                        "id": f"call_{rng.getrandbits(96):024x}",
                        "name": tool,
                        "args": {"query": arg, "limit": rng.randint(1, 20)},
                        "result": _tool_result(rng, tool),
                    }
                )
            rounds.append({"thought": thought, "calls": calls})
        turns.append(
            {
                "user": _slice(rng, prose, _lognorm(rng, 250, 0.9, 20, 4000)),
                "rounds": rounds,
                "final": _slice(rng, prose, _lognorm(rng, 900, 0.8, 50, 6000)),
                "ids": [f"msg_{rng.getrandbits(96):024x}" for _ in range(r + 2)],
            }
        )
        rem -= 3 + 2 * r
        t += 1
    return turns


# ---------------------------------------------------------------- graph

from typing_extensions import TypedDict as _TD
from langgraph.channels.delta import DeltaChannel
from langgraph.pregel._messages import _messages_delta_reducer


class DeltaState(_TD):
    messages: Annotated[list, DeltaChannel(_messages_delta_reducer)]


RESULTS: dict[str, str] = {}  # tool_call id -> result text (ToolNode looks it up)
SCRIPTS: dict[str, Any] = {}  # thread_id -> iterator of AIMessages for the current turn


def _usage(rng_tokens: int) -> dict:
    return {
        "input_tokens": rng_tokens,
        "output_tokens": max(1, rng_tokens // 20),
        "total_tokens": rng_tokens + max(1, rng_tokens // 20),
        "input_token_details": {"cache_read": rng_tokens // 2},
        "output_token_details": {"reasoning": 0},
    }


def ai_messages_for_turn(turn: dict, history_chars: int):
    from langchain_core.messages import AIMessage

    out = []
    for k, rd in enumerate(turn["rounds"]):
        out.append(
            AIMessage(
                content=rd["thought"],
                id=turn["ids"][k + 1],
                tool_calls=[{"id": c["id"], "name": c["name"], "args": c["args"], "type": "tool_call"} for c in rd["calls"]],
                response_metadata={"model_name": MODEL, "finish_reason": "tool_calls", "system_fingerprint": "fp_" + turn["ids"][k + 1][-10:]},
                usage_metadata=_usage(history_chars // 4),
            )
        )
    out.append(
        AIMessage(
            content=turn["final"],
            id=turn["ids"][-1],
            response_metadata={"model_name": MODEL, "finish_reason": "stop", "system_fingerprint": "fp_" + turn["ids"][-1][-10:]},
            usage_metadata=_usage(history_chars // 4),
        )
    )
    return out


def build_graph(mode: str, checkpointer):
    """mode 'default': MessagesState as shipped (add_messages, full list per checkpoint).
    mode 'delta': the opt-in beta DeltaChannel with LangGraph's own batch reducer."""
    from typing_extensions import TypedDict
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.message import MessagesState
    from langgraph.prebuilt import ToolNode, tools_condition

    if mode == "default":
        State = MessagesState
    else:
        State = DeltaState  # type: ignore[assignment]

    def make_tool(name):
        def _t(query: str, limit: int, tool_call_id: Annotated[str, InjectedToolCallId]) -> str:
            return RESULTS[tool_call_id]

        return StructuredTool.from_function(_t, name=name, description=f"{name}: look something up.")

    tools_node = ToolNode([make_tool(n) for n in TOOLS])

    def agent(state, config):
        tid = config["configurable"]["thread_id"]
        return {"messages": [next(SCRIPTS[tid])]}

    g = StateGraph(State)
    g.add_node("agent", agent)
    g.add_node("tools", tools_node)
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", tools_condition, {"tools": "tools", END: END})
    g.add_edge("tools", "agent")
    return g.compile(checkpointer=checkpointer)


def run_thread(graph, thread_id: str, script: list[dict]) -> None:
    from langchain_core.messages import HumanMessage

    hist = 0
    for turn in script:
        for rd in turn["rounds"]:
            for c in rd["calls"]:
                RESULTS[c["id"]] = c["result"]
        hist += len(turn["user"])
        SCRIPTS[thread_id] = iter(ai_messages_for_turn(turn, hist))
        graph.invoke(
            {"messages": [HumanMessage(content=turn["user"], id=turn["ids"][0])]},
            {"configurable": {"thread_id": thread_id}},
        )
        hist += len(turn["final"]) + sum(len(c["result"]) for rd in turn["rounds"] for c in rd["calls"])
