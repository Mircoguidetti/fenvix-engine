"""A real compiled LangGraph agent graph with a scripted model, and a timing wrapper around the checkpointer.

Why a real graph: the checkpoints must be written exactly the way LangGraph writes them (which channels, how many
checkpoints per turn, put_writes before put, metadata, versions). So bench.py does not build Checkpoint objects itself:
it calls graph.invoke() on a StateGraph(MessagesState) with an "agent" node and a "tools" node, the standard ReAct shape,
and LangGraph calls the saver. The only fake part is the model: the agent node returns the next scripted message from
generator.turn_messages() instead of calling an LLM (no network, deterministic sizes).

Per turn LangGraph writes 1 checkpoint for the input, 1 per agent step and 1 per tools step:
2 + 2 * tool_rounds checkpoints, each with the WHOLE message list (MessagesState uses the add_messages reducer, a full-copy
channel). LangGraph 1.2 also has a beta DeltaChannel that stores only deltas; it is not the default, so it is not used.

TimedSaver wraps any BaseCheckpointSaver and records, per call: operation, thread, latency, and logical bytes (the
payload LangGraph handed to the saver, serialized with the saver's own serializer; serialization for the byte count is
done outside the timed region).
"""
from __future__ import annotations

import time
from typing import Any, Iterator, Sequence

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, MessagesState, StateGraph

import generator


class TimedSaver(BaseCheckpointSaver):
    """Delegates every call to the real saver and times it. `sink(record)` receives one dict per call."""

    def __init__(self, inner: BaseCheckpointSaver, sink, step_sink=None):
        super().__init__(serde=inner.serde)
        self.inner = inner
        self.sink = sink
        self.step_sink = step_sink
        self.context: dict = {}  # set by the runner: phase, gap kind of the current turn, etc.
        self._pending_writes_s = 0.0  # put_writes time since the last put: part of "save one step"
        self._pending_writes_n = 0

    # -- helpers ----------------------------------------------------------------------------------------------------
    def _rec(self, op: str, thread: str, dt: float, **kw):
        r = {"ts": time.time(), "op": op, "thread_id": thread, "ms": round(dt * 1000, 3)} | self.context | kw
        self.sink(r)

    def _size(self, obj) -> int:
        try:
            return len(self.serde.dumps_typed(obj)[1])
        except Exception:  # noqa: BLE001 - a value the serializer cannot type: count nothing rather than fail the run
            return 0

    # -- write path -------------------------------------------------------------------------------------------------
    def put(self, config, checkpoint, metadata, new_versions):
        thread = config["configurable"]["thread_id"]
        logical = self._size(checkpoint) + self._size(metadata)
        msgs = self._size(checkpoint.get("channel_values", {}).get("messages", []))
        t0 = time.perf_counter()
        out = self.inner.put(config, checkpoint, metadata, new_versions)
        dt = time.perf_counter() - t0
        self._rec("put", thread, dt, logical_bytes=logical, message_bytes=msgs)
        # "save one step" = the put_writes of the superstep + its put
        self._rec("save_step", thread, dt + self._pending_writes_s, logical_bytes=logical, writes_calls=self._pending_writes_n)
        self._pending_writes_s, self._pending_writes_n = 0.0, 0
        return out

    def put_writes(self, config, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""):
        thread = config["configurable"]["thread_id"]
        logical = sum(self._size(v) for _, v in writes)
        t0 = time.perf_counter()
        out = self.inner.put_writes(config, writes, task_id, task_path)
        dt = time.perf_counter() - t0
        self._pending_writes_s += dt
        self._pending_writes_n += 1
        self._rec("put_writes", thread, dt, logical_bytes=logical, n_writes=len(writes))
        return out

    # -- read path --------------------------------------------------------------------------------------------------
    def get_tuple(self, config):
        thread = config["configurable"]["thread_id"]
        t0 = time.perf_counter()
        out = self.inner.get_tuple(config)
        dt = time.perf_counter() - t0
        op = "time_travel" if config["configurable"].get("checkpoint_id") else "resume"
        self._rec(op, thread, dt, found=out is not None)
        return out

    def list(self, config, *, filter=None, before=None, limit=None) -> Iterator:  # noqa: A002 - LangGraph's signature
        thread = (config or {}).get("configurable", {}).get("thread_id")
        t0 = time.perf_counter()
        items = list(self.inner.list(config, filter=filter, before=before, limit=limit))  # materialize: time the whole read
        dt = time.perf_counter() - t0
        self._rec("list" if limit is None else f"list_limit{limit}", thread, dt, n=len(items))
        return iter(items)

    def delete_thread(self, thread_id: str) -> None:
        t0 = time.perf_counter()
        self.inner.delete_thread(thread_id)
        self._rec("delete_thread", thread_id, time.perf_counter() - t0)

    # -- everything else LangGraph needs, unchanged -----------------------------------------------------------------
    def get_next_version(self, current, channel):
        return self.inner.get_next_version(current, channel)

    def get_delta_channel_history(self, *, config, channels):
        return self.inner.get_delta_channel_history(config=config, channels=channels)

    @property
    def config_specs(self) -> list:
        return self.inner.config_specs


# ----------------------------------------------------------------------------------------------------------------------
# The agent graph
# ----------------------------------------------------------------------------------------------------------------------


class Script:
    """Scripted turns, keyed by thread id; the runner sets the current turn before invoking the graph."""

    def __init__(self):
        self.turn: dict[str, dict] = {}


def _usage(r_text: str) -> dict:
    # what a real chat model attaches to each AI message (token usage, model, finish reason); ~250 bytes serialized
    n = max(1, len(r_text) // 4)
    return {"input_tokens": 1200 + n, "output_tokens": n, "total_tokens": 1200 + 2 * n}


def build_graph(saver: BaseCheckpointSaver, script: Script):
    def agent(state: MessagesState, config) -> dict:
        tid = config["configurable"]["thread_id"]
        turn = script.turn[tid]
        # tool messages since the last human message tell which scripted step comes next
        done = 0
        for m in reversed(state["messages"]):
            if isinstance(m, HumanMessage):
                break
            if isinstance(m, ToolMessage):
                done += 1
        meta = {"model_name": "scripted-model-1", "finish_reason": "stop", "system_fingerprint": "fp_bench0001"}
        if done < len(turn["tools"]):
            tl = turn["tools"][done]
            return {"messages": [AIMessage(content="", tool_calls=[{"id": tl["id"], "name": tl["name"], "args": tl["args"]}],
                                           response_metadata=meta | {"finish_reason": "tool_calls"},
                                           usage_metadata=_usage(str(tl["args"])))]}
        return {"messages": [AIMessage(content=turn["answer"], response_metadata=meta, usage_metadata=_usage(turn["answer"]))]}

    def tools(state: MessagesState, config) -> dict:
        tid = config["configurable"]["thread_id"]
        last = state["messages"][-1]
        by_id = {t["id"]: t for t in script.turn[tid]["tools"]}
        return {"messages": [ToolMessage(content=by_id[c["id"]]["output"], tool_call_id=c["id"], name=c["name"])
                             for c in last.tool_calls]}

    def route(state: MessagesState) -> str:
        last = state["messages"][-1]
        return "tools" if getattr(last, "tool_calls", None) else END

    g = StateGraph(MessagesState)
    g.add_node("agent", agent)
    g.add_node("tools", tools)
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", route, ["tools", END])
    g.add_edge("tools", "agent")
    return g.compile(checkpointer=saver)


def run_turn(graph, script: Script, seed: int, th: generator.ThreadPlan, turn: int) -> None:
    """One user turn: the user message goes in, LangGraph runs agent/tools supersteps and checkpoints each one.
    durability="sync": each checkpoint is written before the next superstep starts (no background writes), so every
    measured save is complete when it is recorded."""
    t = generator.turn_messages(seed, th, turn)
    script.turn[th.thread_id] = t
    graph.invoke({"messages": [HumanMessage(content=t["user"])]},
                 config={"configurable": {"thread_id": th.thread_id}}, durability="sync")
