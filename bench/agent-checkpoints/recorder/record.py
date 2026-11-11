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
Output: OUT/<task_id>.jsonl.gz (one event per line, the last one {"ev": "end", "status": "done"}) and OUT/summary.json
(tokens, dollars, checkpoints, bytes, the status of every conversation).
aws-fixes (B11 of the total review, 2026-11-11):
  - a conversation that fails (an API error, a refusal, more model calls in one user turn than --max-steps allow, any
    exception) is recorded as FAILED with the dollars it spent; its partial file is renamed
    <task_id>.failed.incomplete.gz, so replay.py never takes it for a complete conversation; the run goes on with the
    next conversation and stops after --max-consecutive-failures failures in a row (2: a broken setup is not paid for
    conversation after conversation);
  - summary.json is written after every conversation and at the end whatever happens (spent dollars included);
  - the run RESUMES: run the same command again and every conversation already recorded (status done, its file
    present) is skipped, so it is never paid again; failed ones are retried only with --retry-failed; the spend of the
    earlier runs counts toward --max-dollars;
  - the cap is checked at EVERY model call: after each call (its cost added) and before each call (the spend so far plus
    the most expensive call of this run must stay under the cap), so a run ends within one call of the cap, not after a
    whole user turn of up to --max-steps calls; a conversation cut there is "stopped_by_cap" (incomplete, not replayed);
  - --fake-price MODEL counts the fake model's calls at MODEL's prices (tests of the cap at $0).
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


class BudgetStop(Exception):
    """The spend cap (--max-dollars) is reached: no more model calls."""


def build_graph(task: Task, model, saver, on_call, before_call=lambda: None):
    def agent(state: MessagesState):
        before_call()   # raises BudgetStop before a call the cap cannot pay for
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


def write_summary(out, summary):
    n_done = [c for c in summary["conversations"] if c.get("status") == "done"]
    n = len(n_done) or 1
    tot = lambda k: sum(c["usage"][k] for c in n_done)
    summary["per_conversation"] = {
        "model_calls": round(sum(c["model_calls"] for c in n_done) / n, 2),
        "checkpoints": round(sum(c["puts"] for c in n_done) / n, 2),
        "checkpoint_mb": round(sum(c["checkpoint_bytes"] for c in n_done) / n / 1e6, 4),
        "input_tokens": tot("input_tokens") // n, "output_tokens": tot("output_tokens") // n}
    summary["status_counts"] = {st: sum(1 for c in summary["conversations"] if c.get("status") == st)
                                for st in sorted({c.get("status") for c in summary["conversations"]})}
    tmp = os.path.join(out, ".summary.json.tmp")
    json.dump(summary, open(tmp, "w"), indent=1)
    os.replace(tmp, os.path.join(out, "summary.json"))


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
    ap.add_argument("--retry-failed", action="store_true", help="on a resumed run, record failed conversations again")
    ap.add_argument("--max-consecutive-failures", type=int, default=2)
    ap.add_argument("--fake-price", default="", help="with --model fake: count the calls at this model's prices (tests)")
    a = ap.parse_args()
    if a.pilot:
        a.support, a.coding = 5, 5
    os.makedirs(a.out, exist_ok=True)
    paid = a.model != "fake" or bool(a.fake_price)
    price_model = a.model if a.model != "fake" else (a.fake_price or "claude-opus-5-5")
    old = {}
    sp = os.path.join(a.out, "summary.json")
    if os.path.exists(sp):
        old = json.load(open(sp))
        if old.get("model") != a.model or old.get("seed") != a.seed:
            sys.exit(f"REFUSED: {a.out} holds a recording of model {old.get('model')} seed {old.get('seed')}: a resumed "
                     f"run must use the same model and seed (or another folder)")
    summary = {"model": a.model, "effort": a.effort, "seed": a.seed, "max_dollars": a.max_dollars,
               "conversations": old.get("conversations", []), "runs": old.get("runs", 0) + 1}
    by_id = {c["task_id"]: c for c in summary["conversations"]}
    state = {"spent": float(old.get("spent_dollars", 0.0)), "max_call": 0.0}
    print(f"recording into {a.out}: run {summary['runs']}, {len(by_id)} conversations already in the summary, "
          f"{state['spent']:.4f} $ already spent", flush=True)

    def before_call():
        if paid and state["spent"] + state["max_call"] >= a.max_dollars:
            raise BudgetStop(f"{state['spent']:.4f} $ spent + the most expensive call so far {state['max_call']:.4f} $ "
                             f">= --max-dollars {a.max_dollars}")

    plan = [("support", i) for i in range(a.support)] + [("coding", i) for i in range(a.coding)]
    fails_in_row, stop_reason = 0, None
    try:
        for kind, i in plan:
            tid = f"{kind}-{a.seed}-{i}"   # tasks.make_task's id, known before the task (a coding task makes a repository)
            done_f = os.path.join(a.out, f"{tid}.jsonl.gz")
            prev = by_id.get(tid)
            if prev and prev.get("status") == "done" and os.path.exists(done_f):
                print(f"{tid}: already recorded, skipped (not paid again)", flush=True)
                continue
            if prev and prev.get("status") != "done" and not a.retry_failed:
                print(f"{tid}: {prev.get('status')} in an earlier run, skipped (--retry-failed records it again)", flush=True)
                continue
            task = make_task(kind, i, a.seed)
            assert task.task_id == tid
            part_f = os.path.join(a.out, f"{task.task_id}.part.gz")
            f = gzip.open(part_f, "wt")
            stats = {"task_id": task.task_id, "kind": kind, "status": "running", "turns": 0, "model_calls": 0, "puts": 0,
                     "put_writes": 0, "checkpoint_bytes": 0,
                     "usage": {"input_tokens": 0, "output_tokens": 0, "cache_creation_input_tokens": 0,
                               "cache_read_input_tokens": 0}, "dollars": 0.0}

            def sink(ev):
                f.write(json.dumps(ev) + "\n")
                if ev["ev"] == "model_call":
                    stats["model_calls"] += 1
                    for k, v in ev["usage"].items():
                        stats["usage"][k] += v
                    d = cost.call_dollars(price_model, ev["usage"])
                    stats["dollars"] += d
                    if paid:
                        state["spent"] += d
                        state["max_call"] = max(state["max_call"], d)
                        if state["spent"] >= a.max_dollars:
                            raise BudgetStop(f"{state['spent']:.4f} $ spent >= --max-dollars {a.max_dollars}")
                elif ev["ev"] == "put":
                    stats["puts"] += 1
                    stats["checkpoint_bytes"] += len(ev["checkpoint"][1]) * 3 // 4
                elif ev["ev"] == "put_writes":
                    stats["put_writes"] += 1

            saver = RecordingSaver(sink)
            model = FakeModel(task, a.seed * 100 + i) if a.model == "fake" else ClaudeModel(a.model, a.effort)
            graph = build_graph(task, model, saver, sink, before_call)
            cfg = {"configurable": {"thread_id": task.task_id}, "recursion_limit": 2 * a.max_steps + 2}
            try:
                for turn in task.user_turns:
                    sink({"ev": "user_turn", "t": round(time.time() - saver.t0, 4), "chars": len(turn)})
                    graph.invoke({"messages": [HumanMessage(content=turn)]}, cfg)
                    stats["turns"] += 1
                stats["status"] = "done"
            except BudgetStop as e:
                stats["status"], stats["error"] = "stopped_by_cap", str(e)
                stop_reason = f"STOP: the spend cap: {e}"
            except Exception as e:  # noqa: BLE001 - recorded with its spend, never silent
                stats["status"], stats["error"] = "failed", f"{type(e).__name__}: {str(e)[:400]}"
            f.write(json.dumps({"ev": "end", "status": stats["status"], "error": stats.get("error")}) + "\n")
            f.close()
            if stats["status"] == "done":
                os.replace(part_f, done_f)
            else:
                os.replace(part_f, os.path.join(a.out, f"{task.task_id}.{stats['status'].replace('_', '-')}.incomplete.gz"))
            stats["dollars"] = round(stats["dollars"], 5)
            if prev:   # an earlier attempt (failed or cut): its spend stays in the record
                stats["earlier_attempts"] = prev.get("earlier_attempts", []) + [{k: prev.get(k) for k in ("status", "error", "dollars", "model_calls")}]
                summary["conversations"] = [c for c in summary["conversations"] if c["task_id"] != task.task_id]
            summary["conversations"].append(stats)
            by_id[task.task_id] = stats
            summary["spent_dollars"] = round(state["spent"], 5)
            write_summary(a.out, summary)
            print(json.dumps({k: v for k, v in stats.items() if k != "earlier_attempts"}), flush=True)
            if stop_reason:
                break
            fails_in_row = fails_in_row + 1 if stats["status"] == "failed" else 0
            if fails_in_row >= a.max_consecutive_failures:
                stop_reason = f"STOP: {fails_in_row} conversations failed in a row (last: {stats.get('error')})"
                break
    finally:
        summary["spent_dollars"] = round(state["spent"], 5)
        summary["stop"] = stop_reason
        write_summary(a.out, summary)
    if stop_reason:
        print(stop_reason)
    planned = {f"{k}-{a.seed}-{i}" for k, i in plan}
    done = sum(1 for c in summary["conversations"] if c["task_id"] in planned and c.get("status") == "done")
    print(f"RECORDED: {done} of {len(planned)} planned conversations done, {summary['status_counts']}, "
          f"{summary['per_conversation']}, spent {state['spent']:.4f} $ in all runs")
    sys.exit(0 if done == len(planned) else 1)


if __name__ == "__main__":
    main()
