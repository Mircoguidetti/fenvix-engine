"""Part C · record real agent traffic ONCE (record once, replay many: RULES.md 2026-11-09).

A real LangGraph agent (StateGraph(MessagesState), "agent" and "tools" nodes, the standard ReAct shape) works on the
public-style tasks of tasks.py. Every checkpoint LangGraph writes goes through RecordingSaver, which stores the exact
call (put / put_writes, serialized with LangGraph's own serializer) with its time since the start of the conversation;
every model call is stored with its token usage and latency. replay.py later sends the same stream to Fenvix, Aurora,
Supabase, Atlas... with no model call at all.

Models (`--model`):
  fake                 no network, $0: scripted replies with realistic shapes (tool calls, text sizes); used to test the
                       whole recorder locally and to ESTIMATE tokens per conversation (characters / 4);
  claude-opus-5-5 | claude-sonnet-5-5 | claude-haiku-5-5
                       the official Anthropic SDK (`anthropic`), Messages API with tools, adaptive thinking (the model's
                       default; thinking blocks are passed back unchanged), `output_config.effort` set explicitly, prompt
                       caching on the conversation prefix, server-side refusal fallbacks (`fallbacks: "default"`).
                       Needs ANTHROPIC_API_KEY (GitHub secret). Stops at --max-dollars (computed from the DECLARED prices
                       in cost.py and each response's usage) and at the first API error (no blind retries of a broken run).
Modes: `--pilot` = 5 support + 5 coding conversations; otherwise `--support N --coding M`.
Output: OUT/<task_id>.jsonl.gz (one event per line) and OUT/summary.json (tokens, dollars, checkpoints, bytes).
Usage: record.py OUT --model fake|claude-... [--pilot] [--support N] [--coding M] [--seed 7] [--effort medium] [--max-dollars 5]"""
from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage  # noqa: E402
from langgraph.checkpoint.base import BaseCheckpointSaver  # noqa: E402
from langgraph.checkpoint.memory import InMemorySaver  # noqa: E402
from langgraph.graph import END, START, MessagesState, StateGraph  # noqa: E402

import claude_cost as cost  # noqa: E402  (recorder/claude_cost.py: the DECLARED prices)
from tasks import Task, make_task  # noqa: E402


def plain(config):  # the configurable part LangGraph passes the saver: thread, namespace, parent checkpoint id
    return {"configurable": {k: v for k, v in dict(config.get("configurable", {})).items() if isinstance(v, (str, int, float, bool))}}


def b64(t):  # (type, bytes) from serde.dumps_typed -> JSON-safe
    return [t[0], base64.b64encode(t[1]).decode()]


class RecordingSaver(BaseCheckpointSaver):
    """Delegates to an in-memory saver (the graph needs real reads) and records every write call exactly."""

    def __init__(self, sink):
        self.inner = InMemorySaver()
        super().__init__(serde=self.inner.serde)
        self.sink, self.t0 = sink, time.time()

    def get_tuple(self, config):
        return self.inner.get_tuple(config)

    def list(self, config, **kw):
        return self.inner.list(config, **kw)

    def put(self, config, checkpoint, metadata, new_versions):
        self.sink({"ev": "put", "t": round(time.time() - self.t0, 4), "config": plain(config), "new_versions": dict(new_versions),
                   "checkpoint": b64(self.serde.dumps_typed(checkpoint)), "metadata": b64(self.serde.dumps_typed(metadata))})
        return self.inner.put(config, checkpoint, metadata, new_versions)

    def put_writes(self, config, writes, task_id, task_path=""):
        self.sink({"ev": "put_writes", "t": round(time.time() - self.t0, 4), "config": plain(config), "task_id": task_id,
                   "task_path": task_path, "writes": [[c, b64(self.serde.dumps_typed(v))] for c, v in writes]})
        return self.inner.put_writes(config, writes, task_id, task_path)

    def delete_thread(self, thread_id):
        return self.inner.delete_thread(thread_id)


def to_api(messages):
    """LangGraph state -> Anthropic messages. Assistant content is the API's own blocks, unchanged (thinking included);
    consecutive tool results go back in ONE user message (parallel tool calls)."""
    out = []
    for m in messages:
        if isinstance(m, HumanMessage):
            out.append({"role": "user", "content": [{"type": "text", "text": m.content}]})
        elif isinstance(m, AIMessage):
            out.append({"role": "assistant", "content": m.content})
        elif isinstance(m, ToolMessage):
            block = {"type": "tool_result", "tool_use_id": m.tool_call_id, "content": m.content}
            if out and out[-1]["role"] == "user" and out[-1]["content"] and out[-1]["content"][0].get("type") == "tool_result":
                out[-1]["content"].append(block)
            else:
                out.append({"role": "user", "content": [block]})
    return out


class FakeModel:
    """Scripted replies with the shapes of real ones. Usage = characters / 4 (ESTIMATED tokens)."""

    def __init__(self, task: Task, seed: int):
        self.task, self.rng, self.calls = task, random.Random(seed), 0

    def __call__(self, system, tools, messages):
        self.calls += 1
        last = messages[-1]["content"]
        after_tools = last and last[0].get("type") == "tool_result"
        # tool rounds since the user's last own message, and this turn's target (fixed once per turn)
        last_user = max(k for k, m in enumerate(messages) if m["role"] == "user" and m["content"][0].get("type") == "text")
        turn_tools = sum(1 for m in messages[last_user:] if m["role"] == "user" and m["content"][0].get("type") == "tool_result")
        self.targets = getattr(self, "targets", {})
        target = self.targets.setdefault(last_user, self.rng.randint(1, 3))
        text = lambda n: " ".join(self.rng.choice(["order", "status", "the", "your", "refund", "delivered", "address", "we",
                                                     "test", "function", "fixed", "result", "is", "now", "updated"]) for _ in range(n))
        blocks = []
        if self.task.kind == "support" and turn_tools < target:
            o = self.rng.choice(list(self.task.state["orders"]))
            name, inp = self.rng.choice([("list_orders", {"customer_id": self.task.state["orders"][o]["customer_id"]}),
                                         ("get_order", {"order_id": o})])
            blocks = [{"type": "text", "text": text(12)}, {"type": "tool_use", "id": f"toolu_{self.calls:04d}", "name": name, "input": inp}]
        elif self.task.kind == "coding" and last_user == 0 and turn_tools < 4:
            mod = next(f for f in os.listdir(self.task.state["root"]) if not f.startswith("test_") and f.endswith(".py"))
            steps = [("list_files", {}), ("read_file", {"path": mod}),
                     ("write_file", {"path": mod, "content": self.task.state["fix"]}), ("run_tests", {})]
            name, inp = steps[min(turn_tools, 3)]
            blocks = [{"type": "text", "text": text(10)}, {"type": "tool_use", "id": f"toolu_{self.calls:04d}", "name": name, "input": inp}]
        else:
            blocks = [{"type": "text", "text": text(self.rng.randint(40, 160))}]
        prompt_chars = len(system) + len(json.dumps(tools)) + len(json.dumps(messages))
        usage = {"input_tokens": prompt_chars // 4, "output_tokens": len(json.dumps(blocks)) // 4,
                 "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
        return blocks, usage, "fake"


class ClaudeModel:
    """The official Anthropic SDK. Prompt caching (top-level automatic breakpoint), effort set explicitly, refusal
    fallbacks server-side. Returns the content blocks as plain dicts, passed back unchanged on the next call."""

    def __init__(self, model: str, effort: str):
        import anthropic
        self.client, self.model, self.effort = anthropic.Anthropic(), model, effort

    def __call__(self, system, tools, messages):
        kw = dict(model=self.model, max_tokens=16000, system=system, tools=tools, messages=messages,
                  output_config={"effort": self.effort}, cache_control={"type": "ephemeral"})
        if self.model in ("claude-opus-5-5", "claude-sonnet-5-5"):
            r = self.client.beta.messages.create(betas=["server-side-fallback-2026-07-01"], fallbacks="default", **kw)
        else:
            r = self.client.messages.create(**kw)
        if r.stop_reason == "refusal":
            raise RuntimeError(f"refusal: {getattr(r, 'stop_details', None)}")
        blocks = [b.model_dump(mode="json", exclude_none=True) for b in r.content]
        u = r.usage
        usage = {"input_tokens": u.input_tokens, "output_tokens": u.output_tokens,
                 "cache_creation_input_tokens": u.cache_creation_input_tokens or 0,
                 "cache_read_input_tokens": u.cache_read_input_tokens or 0}
        return blocks, usage, r.model


def build_graph(task: Task, model, saver, on_call):
    def agent(state: MessagesState):
        t0 = time.time()
        blocks, usage, served = model(task.system, task.tools, to_api(state["messages"]))
        on_call({"ev": "model_call", "latency_s": round(time.time() - t0, 3), "usage": usage, "model": served})
        calls = [{"name": b["name"], "args": b["input"], "id": b["id"]} for b in blocks if b.get("type") == "tool_use"]
        return {"messages": [AIMessage(content=blocks, tool_calls=calls, response_metadata={"usage": usage})]}

    def tools(state: MessagesState):
        last = state["messages"][-1]
        return {"messages": [ToolMessage(content=task.call(c["name"], c["args"]), tool_call_id=c["id"]) for c in last.tool_calls]}

    g = StateGraph(MessagesState)
    g.add_node("agent", agent)
    g.add_node("tools", tools)
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", lambda s: "tools" if s["messages"][-1].tool_calls else END, {"tools": "tools", END: END})
    g.add_edge("tools", "agent")
    return g.compile(checkpointer=saver)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--model", default="fake")
    ap.add_argument("--pilot", action="store_true")
    ap.add_argument("--support", type=int, default=0)
    ap.add_argument("--coding", type=int, default=0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--effort", default="medium")
    ap.add_argument("--max-dollars", type=float, default=5.0)
    ap.add_argument("--max-steps", type=int, default=30, help="model calls per user turn, at most")
    a = ap.parse_args()
    if a.pilot:
        a.support, a.coding = 5, 5
    os.makedirs(a.out, exist_ok=True)
    spent, summary = 0.0, {"model": a.model, "effort": a.effort, "seed": a.seed, "conversations": []}
    plan = [("support", i) for i in range(a.support)] + [("coding", i) for i in range(a.coding)]
    for kind, i in plan:
        task = make_task(kind, i, a.seed)
        f = gzip.open(os.path.join(a.out, f"{task.task_id}.jsonl.gz"), "wt")
        stats = {"task_id": task.task_id, "kind": kind, "turns": 0, "model_calls": 0, "puts": 0, "put_writes": 0,
                 "checkpoint_bytes": 0, "usage": {"input_tokens": 0, "output_tokens": 0, "cache_creation_input_tokens": 0,
                                                  "cache_read_input_tokens": 0}, "dollars": 0.0}

        def sink(ev):
            nonlocal spent
            f.write(json.dumps(ev) + "\n")
            if ev["ev"] == "model_call":
                stats["model_calls"] += 1
                for k, v in ev["usage"].items():
                    stats["usage"][k] += v
                d = cost.call_dollars(a.model if a.model != "fake" else "claude-opus-5-5", ev["usage"])
                stats["dollars"] += d
                if a.model != "fake":
                    spent += d
            elif ev["ev"] == "put":
                stats["puts"] += 1
                stats["checkpoint_bytes"] += len(ev["checkpoint"][1]) * 3 // 4
            else:
                stats["put_writes"] += 1

        saver = RecordingSaver(sink)
        model = FakeModel(task, a.seed * 100 + i) if a.model == "fake" else ClaudeModel(a.model, a.effort)
        graph = build_graph(task, model, saver, sink)
        cfg = {"configurable": {"thread_id": task.task_id}, "recursion_limit": 2 * a.max_steps + 2}
        for turn in task.user_turns:
            sink({"ev": "user_turn", "t": round(time.time() - saver.t0, 4), "chars": len(turn)})
            graph.invoke({"messages": [HumanMessage(content=turn)]}, cfg)
            stats["turns"] += 1
            if spent >= a.max_dollars:
                break
        f.close()
        stats["dollars"] = round(stats["dollars"], 5)
        summary["conversations"].append(stats)
        print(json.dumps(stats), flush=True)
        if spent >= a.max_dollars:
            print(f"STOP: {spent:.2f} $ spent >= --max-dollars {a.max_dollars}")
            break
    n = len(summary["conversations"]) or 1
    tot = lambda k: sum(c["usage"][k] for c in summary["conversations"])
    summary.update({"spent_dollars": round(spent, 4), "per_conversation": {
        "model_calls": round(sum(c["model_calls"] for c in summary["conversations"]) / n, 2),
        "checkpoints": round(sum(c["puts"] for c in summary["conversations"]) / n, 2),
        "checkpoint_mb": round(sum(c["checkpoint_bytes"] for c in summary["conversations"]) / n / 1e6, 4),
        "input_tokens": tot("input_tokens") // n, "output_tokens": tot("output_tokens") // n}})
    json.dump(summary, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    print(f"RECORDED: {len(summary['conversations'])} conversations, {summary['per_conversation']}, spent {spent:.4f} $")


if __name__ == "__main__":
    main()
