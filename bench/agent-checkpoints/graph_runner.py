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

import copy
import hashlib
import threading
import time
from collections import Counter
from typing import Any, Iterator, Sequence

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, MessagesState, StateGraph

import generator


def _digest(typed: tuple[str, bytes]) -> bytes:
    return hashlib.blake2b(typed[0].encode() + b"\0" + bytes(typed[1]), digest_size=16).digest()


def canonical(obj):
    """Plain dicts with their keys in sorted order, recursively (lists and tuples walked, every other object as is).
    The Postgres saver gives back a checkpoint whose dict keys come in another order than LangGraph wrote them (it
    rebuilds the dict from columns and blobs); the content is the same. Hashing the canonical form compares the values
    byte for byte through the saver's own serializer, and ignores only the order of dict keys."""
    if type(obj) is dict:
        return {k: canonical(obj[k]) for k in sorted(obj, key=str)}
    if type(obj) is list:
        return [canonical(x) for x in obj]
    if type(obj) is tuple:
        return tuple(canonical(x) for x in obj)
    return obj


def hash_of(serde, obj) -> bytes:
    """Hash of an object read back from a saver: its canonical form, serialized with the saver's own serializer."""
    return _digest(serde.dumps_typed(canonical(obj)))


def hash_written(serde, typed: tuple[str, bytes]) -> bytes:
    """Hash of what the saver was given, from the bytes of its own serializer, decoded once and hashed like a read.
    Why the decode: a message object rebuilt from bytes serializes a few bytes longer than the object LangGraph built
    (pydantic's set fields), though the two compare equal; a perfect store returns exactly the decoded bytes, so this is
    the answer every read must match."""
    return hash_of(serde, serde.loads_typed(typed))


class Expected:
    """The correctness check (P-bench-local 2.4): at write time a hash of every checkpoint and every pending write the
    saver was given, serialized with the saver's own serializer (`serde.dumps_typed`); every read in every phase is
    compared with it, outside the timed region. Shared by all workers (one lock). A "wrong read" is one read call or one
    list item whose answer differs from what was written, plus one per written checkpoint a whole-history list leaves
    out. Reads of erased threads are checked by bench.py's erase phase (`threads_left_readable`), not here.
      ck[(thread, ns, checkpoint_id)]          = hash of the checkpoint
      writes[(thread, ns, checkpoint_id)]      = {(task_id, idx): (channel, hash of the value)}
      latest[(thread, ns)]                     = id of the newest checkpoint written
      ids[(thread, ns)]                        = every checkpoint id written"""

    SPECIAL = None  # filled lazily: LangGraph's WRITES_IDX_MAP (special channels overwrite, regular ones keep the first)

    def __init__(self):
        self.lock = threading.Lock()
        self.ck: dict = {}
        self.writes: dict = {}
        self.latest: dict = {}
        self.ids: dict = {}
        self.reads_checked = 0
        self.wrong = Counter()  # kind -> count
        self.examples: list[dict] = []

    # -- write side -------------------------------------------------------------------------------------------------
    def on_put(self, thread: str, ns: str, cid: str, h: bytes) -> None:
        with self.lock:
            self.ck[(thread, ns, cid)] = h
            self.latest[(thread, ns)] = cid
            self.ids.setdefault((thread, ns), set()).add(cid)

    def on_put_writes(self, thread: str, ns: str, cid: str, task_id: str, items: list[tuple[str, int, bytes]]) -> None:
        if Expected.SPECIAL is None:
            from langgraph.checkpoint.base import WRITES_IDX_MAP
            Expected.SPECIAL = dict(WRITES_IDX_MAP)
        with self.lock:
            w = self.writes.setdefault((thread, ns, cid), {})
            for channel, i, h in items:
                idx = Expected.SPECIAL.get(channel, i)
                if (task_id, idx) in w and channel not in Expected.SPECIAL:
                    continue  # the savers keep the first regular write of a (task, idx)
                w[(task_id, idx)] = (channel, h)

    def forget_thread(self, thread: str) -> None:
        with self.lock:
            for d in (self.ck, self.writes):
                for k in [k for k in d if k[0] == thread]:
                    del d[k]
            for d in (self.latest, self.ids):
                for k in [k for k in d if k[0] == thread]:
                    del d[k]

    # -- read side --------------------------------------------------------------------------------------------------
    def _bad(self, kind: str, **info) -> None:
        with self.lock:
            self.wrong[kind] += 1
            if len(self.examples) < 20:
                self.examples.append({"kind": kind} | info)

    def check_tuple(self, serde, tup, thread: str, ns: str, want_id: str | None, latest: bool, op: str) -> None:
        """One CheckpointTuple read back (or None). want_id: the id asked for (time travel); latest: a resume, which
        must return the newest checkpoint written for the thread."""
        with self.lock:
            self.reads_checked += 1
            newest = self.latest.get((thread, ns))
        if tup is None:
            if want_id is not None or (latest and newest is not None):
                self._bad("missing", op=op, thread=thread, want=want_id or newest)
            return
        cid = tup.config["configurable"]["checkpoint_id"]
        if want_id is not None and cid != want_id:
            self._bad("wrong_id", op=op, thread=thread, want=want_id, got=cid)
            return
        if latest and cid != newest:
            self._bad("not_latest", op=op, thread=thread, want=newest, got=cid)
            return
        with self.lock:
            h = self.ck.get((thread, ns, cid))
            exp_w = dict(self.writes.get((thread, ns, cid), {}))
        if h is None:
            self._bad("unknown_checkpoint", op=op, thread=thread, got=cid)
            return
        if tup.checkpoint.get("id") != cid or hash_of(serde, tup.checkpoint) != h:
            self._bad("checkpoint_differs", op=op, thread=thread, checkpoint_id=cid)
            return
        got_w = Counter((t, ch, hash_of(serde, v)) for t, ch, v in (tup.pending_writes or []))
        want_w = Counter((t, ch, hh) for (t, _i), (ch, hh) in exp_w.items())
        if got_w != want_w:
            self._bad("pending_writes_differ", op=op, thread=thread, checkpoint_id=cid,
                      want=sum(want_w.values()), got=sum(got_w.values()))

    def check_list(self, serde, items: list, thread: str, ns: str, whole: bool, op: str, limit: int | None = None) -> None:
        """Every item checked for content; then the list itself (bench-local review F6): ids strictly descending (the
        savers' order, newest first) and unique; a whole list holds exactly the written ids; a list with limit=N holds
        exactly the N newest written ids (min(N, written))."""
        for t in items:
            self.check_tuple(serde, t, thread, ns, t.config["configurable"]["checkpoint_id"], False, op)
        got = [t.config["configurable"]["checkpoint_id"] for t in items]
        if len(set(got)) != len(got):
            self._bad("list_duplicates", op=op, thread=thread, items=len(got), unique=len(set(got)))
        elif any(a <= b for a, b in zip(got, got[1:])):
            self._bad("list_order", op=op, thread=thread)
        with self.lock:
            want = sorted(self.ids.get((thread, ns), ()), reverse=True)
        if whole:
            for cid in sorted(set(want) - set(got)):
                self._bad("left_out_of_list", op=op, thread=thread, checkpoint_id=cid)
        elif limit is not None and got != want[:limit]:
            self._bad("not_the_newest_n", op=op, thread=thread, limit=limit, got=len(got), want=min(limit, len(want)))

    def written_ids(self, thread: str) -> list[tuple[str, str]]:
        """(ns, checkpoint_id) of every checkpoint written for a thread (kept before an erase forgets them)."""
        with self.lock:
            return sorted((ns, cid) for (t, ns), ids in self.ids.items() if t == thread for cid in ids)

    def export_state(self) -> dict:
        """Everything but the lock: a worker process hands its write-side state to the parent (bench.py, --workers N)."""
        with self.lock:
            return {"ck": self.ck, "writes": self.writes, "latest": self.latest, "ids": self.ids,
                    "reads_checked": self.reads_checked, "wrong": dict(self.wrong), "examples": self.examples}

    def merge_state(self, st: dict) -> None:
        with self.lock:
            self.ck.update(st["ck"])
            self.writes.update(st["writes"])
            self.latest.update(st["latest"])
            for k, v in st["ids"].items():
                self.ids.setdefault(k, set()).update(v)
            self.reads_checked += st["reads_checked"]
            self.wrong.update(st["wrong"])
            self.examples.extend(st["examples"][: max(0, 20 - len(self.examples))])

    def report(self) -> dict:
        with self.lock:
            return {"wrong_reads": sum(self.wrong.values()), "by_kind": dict(self.wrong), "reads_checked": self.reads_checked,
                    "checkpoints_written": len(self.ck), "examples": list(self.examples)}


class TimedSaver(BaseCheckpointSaver):
    """Delegates every call to the real saver and times it. `sink(record)` receives one dict per call.
    With `expected` (an Expected), every write is recorded and every read is checked against it."""

    def __init__(self, inner: BaseCheckpointSaver, sink, step_sink=None, expected: Expected | None = None):
        super().__init__(serde=inner.serde)
        self.inner = inner
        self.sink = sink
        self.step_sink = step_sink
        self.expected = expected
        self.context: dict = {}  # set by the runner: phase, gap kind of the current turn, etc.
        self._pending_writes_s = 0.0  # put_writes time since the last put: part of "save one step"
        self._pending_writes_n = 0

    # -- helpers ----------------------------------------------------------------------------------------------------
    def _rec(self, op: str, thread: str, dt: float, **kw):
        r = {"ts": time.time(), "op": op, "thread_id": thread, "ms": round(dt * 1000, 3)} | self.context | kw
        self.sink(r)

    def _size(self, obj) -> int:
        return self._size_hash(obj)[0]

    def _size_hash(self, obj) -> tuple[int, bytes | None]:
        """Logical bytes (the payload as handed to the saver, serialized as before) and, with a check, the hash of its
        canonical form (outside the timed region)."""
        try:
            typed = self.serde.dumps_typed(obj)
        except Exception:  # noqa: BLE001 - a value the serializer cannot type: count nothing rather than fail the run
            return 0, None
        return len(typed[1]), (hash_written(self.serde, typed) if self.expected is not None else None)

    @staticmethod
    def _ns(config) -> str:
        return config["configurable"].get("checkpoint_ns", "")

    def _check_read(self, tup, config, op: str) -> None:
        if self.expected is None or self.context.get("phase") == "erase_verify":
            return
        c = config["configurable"]
        want = c.get("checkpoint_id")
        self.expected.check_tuple(self.serde, tup, c["thread_id"], self._ns(config), want, want is None, op)

    # -- write path -------------------------------------------------------------------------------------------------
    def put(self, config, checkpoint, metadata, new_versions):
        thread = config["configurable"]["thread_id"]
        if self.expected is not None:
            # a frozen copy (outside the timed region): LangGraph's add_messages gives ids in place to message objects
            # that a background put may still be serializing, so without it the stored bytes depend on thread timing
            checkpoint = copy.deepcopy(checkpoint)
        ck_size, ck_hash = self._size_hash(checkpoint)
        logical = ck_size + self._size(metadata)
        msgs = self._size(checkpoint.get("channel_values", {}).get("messages", []))
        t0 = time.perf_counter()
        out = self.inner.put(config, checkpoint, metadata, new_versions)
        dt = time.perf_counter() - t0
        if self.expected is not None:
            self.expected.on_put(thread, self._ns(config), checkpoint["id"], ck_hash)
        self._rec("put", thread, dt, logical_bytes=logical, message_bytes=msgs)
        # "save one step" = the put_writes of the superstep + its put
        self._rec("save_step", thread, dt + self._pending_writes_s, logical_bytes=logical, writes_calls=self._pending_writes_n)
        self._pending_writes_s, self._pending_writes_n = 0.0, 0
        return out

    def put_writes(self, config, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""):
        thread = config["configurable"]["thread_id"]
        if self.expected is not None:
            writes = [(ch, copy.deepcopy(v)) for ch, v in writes]  # frozen copy, as in put
        sized = [self._size_hash(v) for _, v in writes]
        logical = sum(n for n, _ in sized)
        t0 = time.perf_counter()
        out = self.inner.put_writes(config, writes, task_id, task_path)
        dt = time.perf_counter() - t0
        if self.expected is not None:
            self.expected.on_put_writes(thread, self._ns(config), config["configurable"]["checkpoint_id"], task_id,
                                        [(ch, i, h) for i, ((ch, _v), (_n, h)) in enumerate(zip(writes, sized))])
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
        self._check_read(out, config, op)
        return out

    def list(self, config, *, filter=None, before=None, limit=None) -> Iterator:  # noqa: A002 - LangGraph's signature
        thread = (config or {}).get("configurable", {}).get("thread_id")
        t0 = time.perf_counter()
        items = list(self.inner.list(config, filter=filter, before=before, limit=limit))  # materialize: time the whole read
        dt = time.perf_counter() - t0
        op = "list" if limit is None else f"list_limit{limit}"
        self._rec(op, thread, dt, n=len(items))
        if self.expected is not None and thread and self.context.get("phase") != "erase_verify":
            whole = limit is None and before is None and not filter
            self.expected.check_list(self.serde, items, thread, self._ns(config), whole, op,
                                     limit if (before is None and not filter) else None)
        return iter(items)

    def delete_thread(self, thread_id: str) -> None:
        t0 = time.perf_counter()
        self.inner.delete_thread(thread_id)
        self._rec("delete_thread", thread_id, time.perf_counter() - t0)
        if self.expected is not None:
            self.expected.forget_thread(thread_id)

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
