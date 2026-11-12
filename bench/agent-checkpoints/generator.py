"""Workload generator for the agent checkpoint benchmark (Part B).

What it does: builds a deterministic plan of agent activity for a given seed and profile:
  - N end users, each owning several threads (conversations / coding sessions);
  - per thread a heavy-tailed number of turns, each turn = one user message, some tool rounds, one final answer;
  - the turns of a thread are grouped in sessions separated by short (minutes) or long (days) gaps, so most threads
    are idle most of the time and users come back after short and long breaks;
  - all events are placed on a virtual clock and sorted, so the benchmark replays them interleaved across threads.

It also produces the message contents of every turn, deterministically from (seed, thread, message index), so the plan
file stays small and the same seed always gives the same bytes. Contents are word-based text (chat) or code-like text
(coding agent), so they compress roughly like real text, not like random bytes.

The plan is written by bench.py through a REAL compiled LangGraph graph (see graph_runner.py): this file only decides
what the agent "says". Where every parameter comes from is in README.md ("Workload shapes").

Usage:
  python generator.py --profile small --seed 42 --out plan.jsonl      # write the plan
  python generator.py --profile standard --describe                    # print the shape (sizes per thread, quantiles)
"""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import random
import sys
from dataclasses import dataclass

# ----------------------------------------------------------------------------------------------------------------------
# Profiles. Every number here is an assumption of the workload; README.md says where each comes from.
# ----------------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Kind:
    """Shape of one kind of thread."""

    name: str
    turns_median: float  # lognormal median of turns per thread
    turns_sigma: float  # lognormal sigma (heavy tail)
    turns_max: int
    tool_rounds: tuple[int, int]  # tool calls per turn, uniform inclusive range
    user_chars: tuple[int, int]  # user message length, lognormal between p5..p95 roughly
    answer_chars: tuple[int, int]  # final answer length
    tool_out_chars: tuple[int, int]  # tool output length
    code: bool  # tool outputs look like source code


CHAT = Kind("chat", turns_median=7, turns_sigma=0.9, turns_max=80, tool_rounds=(0, 1),
            user_chars=(40, 600), answer_chars=(500, 3500), tool_out_chars=(300, 3000), code=False)
CODING = Kind("coding", turns_median=10, turns_sigma=0.35, turns_max=20, tool_rounds=(2, 4),
              user_chars=(60, 900), answer_chars=(200, 1500), tool_out_chars=(1000, 11000), code=True)


@dataclass(frozen=True)
class Profile:
    users: int
    threads_per_user_mean: float  # geometric: most users own 1-3 threads, a few own many
    threads_per_user_max: int
    coding_share: float  # share of threads that are coding-agent sessions
    sessions_per_thread_mean: float  # how many times the user comes back to the same thread
    short_gap_share: float  # share of returns after a short gap (minutes) vs a long one (days)
    short_gap_s: tuple[int, int]
    long_gap_s: tuple[int, int]
    user_start_window_s: int  # users start uniformly over this window
    think_s: tuple[float, float]  # model "thinking" between agent supersteps (M25 measured 2.3-8 s)
    read_type_s: tuple[float, float]  # user reading/typing between turns of one session


PROFILES = {
    # local validation: a few minutes on a laptop-class sandbox
    "tiny": Profile(users=4, threads_per_user_mean=2, threads_per_user_max=4, coding_share=0.25,
                    sessions_per_thread_mean=1.5, short_gap_share=0.6, short_gap_s=(300, 3600),
                    long_gap_s=(86400, 30 * 86400), user_start_window_s=7 * 86400, think_s=(2.3, 8.0),
                    read_type_s=(10, 120)),
    "small": Profile(users=20, threads_per_user_mean=2.5, threads_per_user_max=8, coding_share=0.15,
                     sessions_per_thread_mean=1.8, short_gap_share=0.6, short_gap_s=(300, 3600),
                     long_gap_s=(86400, 30 * 86400), user_start_window_s=14 * 86400, think_s=(2.3, 8.0),
                     read_type_s=(10, 120)),
    # AWS round: a few thousand threads, a few GB stored, one to two hours of writing per target
    "standard": Profile(users=1000, threads_per_user_mean=2.5, threads_per_user_max=20, coding_share=0.15,
                        sessions_per_thread_mean=1.8, short_gap_share=0.6, short_gap_s=(300, 3600),
                        long_gap_s=(86400, 30 * 86400), user_start_window_s=30 * 86400, think_s=(2.3, 8.0),
                        read_type_s=(10, 120)),
    # P-bench-big (2026-11-12): the standard shapes with twice the users (about 4,900 threads, about 13 GB logical,
    # ESTIMATED from standard's 6.7 GB), the largest set whose three Postgres copies, three mongod copies and our MinIO
    # objects each fit this sandbox's disk (about 25 GB free) one line at a time. Longer histories were not added: the
    # bytes of a thread grow with the square of its length (every checkpoint holds the whole message list), so on this
    # disk they would buy fewer threads, not more data that misses memory.
    "big": Profile(users=2000, threads_per_user_mean=2.5, threads_per_user_max=20, coding_share=0.15,
                   sessions_per_thread_mean=1.8, short_gap_share=0.6, short_gap_s=(300, 3600),
                   long_gap_s=(86400, 30 * 86400), user_start_window_s=30 * 86400, think_s=(2.3, 8.0),
                   read_type_s=(10, 120)),
}

# ----------------------------------------------------------------------------------------------------------------------
# Deterministic text
# ----------------------------------------------------------------------------------------------------------------------

_WORDS = (
    "the of and to a in is it you that he was for on are with as I his they be at one have this from or had by "
    "word but what some we can out other were all there when up use your how said an each she which do their time if "
    "will way about many then them write would like so these her long make thing see him two has look more day could "
    "go come did number sound no most people my over know water than call first who may down side been now find any "
    "new work part take get place made live where after back little only round man year came show every good me give "
    "our under name very through just form sentence great think say help low line differ turn cause much mean before "
    "move right boy old too same tell does set three want air well also play small end put home read hand port large "
    "spell add even land here must big high such follow act why ask men change went light kind off need house picture "
    "try us again animal point mother world near build self earth father order invoice customer account refund policy "
    "shipping delivery booking flight hotel price discount subscription plan upgrade report summary meeting schedule "
    "calendar email draft review contract clause payment balance transaction error retry status update ticket support"
).split()
_CODE = (
    "def class return import from self None True False if else elif for while in not and or try except raise with as "
    "async await yield lambda pass break continue fn let mut pub struct impl enum match Ok Err Some Vec String usize "
    "u64 i64 const static use mod crate super where trait dyn Box Arc Mutex HashMap BTreeMap Option Result config "
    "request response handler client server buffer index offset length count value key table query cursor batch "
    "manifest compaction snapshot checkpoint writer reader stream bytes encode decode parse validate assert expect"
).split()
# Zipf-like weights: frequent words much more frequent, like real text
_WORD_W = [1.0 / (i + 1) ** 0.9 for i in range(len(_WORDS))]
_CODE_W = [1.0 / (i + 1) ** 0.8 for i in range(len(_CODE))]


def rng_for(*parts) -> random.Random:
    """A Random seeded from a stable hash of the parts (Python's hash() is salted per process, sha256 is not)."""
    h = hashlib.sha256(":".join(str(p) for p in parts).encode()).digest()
    return random.Random(int.from_bytes(h[:8], "big"))


def text(r: random.Random, n_chars: int) -> str:
    out, size = [], 0
    while size < n_chars:
        w = r.choices(_WORDS, _WORD_W, k=12)
        s = " ".join(w).capitalize() + ". "
        out.append(s)
        size += len(s)
    return "".join(out)[:n_chars]


def code_text(r: random.Random, n_chars: int) -> str:
    out, size, depth = [], 0, 0
    while size < n_chars:
        depth = max(0, min(4, depth + r.choice((-1, 0, 0, 1))))
        line = "    " * depth + " ".join(r.choices(_CODE, _CODE_W, k=r.randint(2, 9)))
        line += r.choice(("", ":", "(", ")", " {", "}", ";", " = 0", "(self)"))
        out.append(line + "\n")
        size += len(line) + 1
    return "".join(out)[:n_chars]


def lognormal_between(r: random.Random, lo: float, hi: float) -> int:
    """A lognormal draw whose ~5th and ~95th percentiles are lo and hi (clipped to [lo/2, 2*hi])."""
    mu = (math.log(lo) + math.log(hi)) / 2
    sigma = (math.log(hi) - math.log(lo)) / (2 * 1.645)
    return int(min(max(r.lognormvariate(mu, sigma), lo / 2), hi * 2))


# ----------------------------------------------------------------------------------------------------------------------
# Plan
# ----------------------------------------------------------------------------------------------------------------------


@dataclass
class ThreadPlan:
    user: str
    thread_id: str
    kind: str
    turns: int
    tool_rounds: list[int]  # per turn


def thread_id(user: int, t: int) -> str:
    return f"u{user:06d}-t{t:03d}"


def build_threads(seed: int, p: Profile) -> list[ThreadPlan]:
    threads = []
    for u in range(p.users):
        r = rng_for(seed, "user", u)
        # geometric number of threads with the given mean, at least 1
        q = 1.0 / p.threads_per_user_mean
        n = 1
        while n < p.threads_per_user_max and r.random() > q:
            n += 1
        for t in range(n):
            rt = rng_for(seed, "thread", u, t)
            k = CODING if rt.random() < p.coding_share else CHAT
            turns = int(min(k.turns_max, max(1, round(rt.lognormvariate(math.log(k.turns_median), k.turns_sigma)))))
            rounds = [rt.randint(*k.tool_rounds) for _ in range(turns)]
            threads.append(ThreadPlan(f"user{u:06d}", thread_id(u, t), k.name, turns, rounds))
    return threads


def extend_thread(seed: int, th: ThreadPlan, extra: int) -> ThreadPlan:
    """P-bench-big: the same thread with `extra` more turns after its planned ones (the measured mix saves new steps on
    old threads). The tool rounds of the new turns are drawn from (seed, "extra", thread, turn), so they never change the
    planned turns and are the same for every target."""
    k = CODING if th.kind == "coding" else CHAT
    rounds = list(th.tool_rounds)
    for turn in range(len(rounds), len(rounds) + extra):
        rounds.append(rng_for(seed, "extra", th.thread_id, turn).randint(*k.tool_rounds))
    return dataclasses.replace(th, turns=len(rounds), tool_rounds=rounds)


def turn_messages(seed: int, th: ThreadPlan, turn: int) -> dict:
    """The scripted content of one turn: the user message, the AI tool calls with their tool outputs, the answer.
    Deterministic in (seed, thread, turn)."""
    k = CODING if th.kind == "coding" else CHAT
    r = rng_for(seed, "turn", th.thread_id, turn)
    gen = code_text if k.code else text
    tools = []
    for i in range(th.tool_rounds[turn]):
        name = r.choice(("read_file", "run_tests", "grep", "edit_file")) if k.code else r.choice(("search", "lookup_order", "get_calendar"))
        args = {"query": text(r, r.randint(20, 120))} if not k.code else {"path": f"src/{r.choice(_CODE)}/{r.choice(_CODE)}.rs"}
        tools.append({"id": f"call_{th.thread_id}_{turn}_{i}", "name": name, "args": args,
                      "output": gen(r, lognormal_between(r, *k.tool_out_chars))})
    return {
        "user": text(r, lognormal_between(r, *k.user_chars)),
        "tools": tools,
        "answer": text(r, lognormal_between(r, *k.answer_chars)),
    }


def build_events(seed: int, p: Profile, threads: list[ThreadPlan]) -> list[dict]:
    """One event per turn, on a virtual clock. A thread's turns are split into sessions; the first turn of each later
    session is a return after a 'short' or 'long' gap. Virtual times are recorded, never slept on by default."""
    events = []
    for th in threads:
        r = rng_for(seed, "schedule", th.thread_id)
        user_idx = int(th.user[4:])
        t = rng_for(seed, "userstart", user_idx).uniform(0, p.user_start_window_s) + r.uniform(0, 3 * 86400)
        # number of sessions: 1 + geometric, never more than turns
        n_sess = 1
        while n_sess < th.turns and r.random() > 1.0 / p.sessions_per_thread_mean:
            n_sess += 1
        cuts = sorted(r.sample(range(1, th.turns), n_sess - 1)) if n_sess > 1 else []
        gap = "first"
        for turn in range(th.turns):
            if turn in cuts:
                if r.random() < p.short_gap_share:
                    gap, d = "short", r.uniform(*p.short_gap_s)
                else:
                    gap, d = "long", r.uniform(*p.long_gap_s)
                t += d
            elif turn > 0:
                gap = "none"
                t += r.uniform(*p.read_type_s)
            events.append({"t": round(t, 3), "user": th.user, "thread_id": th.thread_id, "kind": th.kind,
                           "turn": turn, "tool_rounds": th.tool_rounds[turn], "gap": gap})
            # supersteps of this turn: input + agent + 2 per tool round; thinking time between them
            t += sum(r.uniform(*p.think_s) for _ in range(1 + 2 * th.tool_rounds[turn]))
    events.sort(key=lambda e: (e["t"], e["thread_id"], e["turn"]))
    return events


def plan(seed: int, profile: str) -> tuple[list[ThreadPlan], list[dict]]:
    p = PROFILES[profile]
    threads = build_threads(seed, p)
    return threads, build_events(seed, p, threads)


# ----------------------------------------------------------------------------------------------------------------------
# Shape description (no database): how big each thread gets if every checkpoint stores the whole message list
# ----------------------------------------------------------------------------------------------------------------------


def thread_shape(seed: int, th: ThreadPlan) -> dict:
    """Content chars of the thread and the sum over its checkpoints of the cumulative message list (what a full-copy
    checkpointer stores, before serializer overhead). Checkpoints per turn = 2 + 2 * tool_rounds (input step, agent,
    then tools+agent per tool round)."""
    cum, stored, ckpts = 0, 0, 0
    for turn in range(th.turns):
        m = turn_messages(seed, th, turn)
        cum += len(m["user"])
        stored += cum  # input checkpoint
        ckpts += 1
        for tl in m["tools"]:
            cum += len(json.dumps(tl["args"])) + 40
            stored += cum
            cum += len(tl["output"])
            stored += cum
            ckpts += 2
        cum += len(m["answer"])
        stored += cum
        ckpts += 1
    return {"content_bytes": cum, "full_copy_bytes": stored, "checkpoints": ckpts}


def quantiles(xs: list[float], qs=(0.5, 0.9, 0.99)) -> dict:
    xs = sorted(xs)
    return {f"p{int(q * 100)}": xs[min(len(xs) - 1, int(q * len(xs)))] for q in qs} | {"max": xs[-1], "n": len(xs)}


def describe(seed: int, profile: str) -> dict:
    threads, events = plan(seed, profile)
    out = {"seed": seed, "profile": profile, "users": PROFILES[profile].users, "threads": len(threads),
           "turns": len(events), "gaps": {g: sum(1 for e in events if e["gap"] == g) for g in ("first", "none", "short", "long")}}
    shapes = {th.thread_id: thread_shape(seed, th) for th in threads}
    for kind in ("chat", "coding"):
        sh = [shapes[th.thread_id] for th in threads if th.kind == kind]
        if not sh:
            continue
        out[kind] = {
            "threads": len(sh),
            "turns": quantiles([th.turns for th in threads if th.kind == kind]),
            "full_copy_MB": {k: (round(v / 1e6, 3) if k != "n" else v) for k, v in quantiles([s["full_copy_bytes"] for s in sh]).items()},
            "full_copy_MB_mean": round(sum(s["full_copy_bytes"] for s in sh) / len(sh) / 1e6, 3),
            "content_KB": {k: (round(v / 1e3, 1) if k != "n" else v) for k, v in quantiles([s["content_bytes"] for s in sh]).items()},
        }
    out["checkpoints_total"] = sum(s["checkpoints"] for s in shapes.values())
    out["full_copy_GB_total"] = round(sum(s["full_copy_bytes"] for s in shapes.values()) / 1e9, 4)
    out["full_copy_MB_mean_all_threads"] = round(out["full_copy_GB_total"] * 1e3 / len(threads), 3)
    return out


def plan_digest(events: list[dict]) -> str:
    return hashlib.sha256("\n".join(json.dumps(e, sort_keys=True) for e in events).encode()).hexdigest()[:16]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", default="small", choices=sorted(PROFILES))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", help="write the plan (one JSON event per line, first line = header)")
    ap.add_argument("--describe", action="store_true", help="print the workload shape")
    ap.add_argument("--export-turns", help="write every turn's scripted contents in plan order (for mastra/bench.mjs)")
    a = ap.parse_args()
    threads, events = plan(a.seed, a.profile)
    if a.describe:
        print(json.dumps(describe(a.seed, a.profile), indent=2))
    if a.export_turns:
        export_turns(a.seed, threads, events, a.export_turns)
    if a.out:
        with open(a.out, "w") as f:
            hdr = {"header": True, "seed": a.seed, "profile": a.profile, "params": dataclasses.asdict(PROFILES[a.profile]),
                   "threads": [dataclasses.asdict(t) for t in threads], "digest": plan_digest(events)}
            f.write(json.dumps(hdr) + "\n")
            for e in events:
                f.write(json.dumps(e) + "\n")
        print(f"plan: {len(threads)} threads, {len(events)} turns, digest {plan_digest(events)} -> {a.out}", file=sys.stderr)


def export_turns(seed: int, threads: list[ThreadPlan], events: list[dict], path: str) -> None:
    """The same turns as the LangGraph run, as plain messages, so a non-Python framework replays identical bytes."""
    by_id = {t.thread_id: t for t in threads}
    with open(path, "w") as f:
        for e in events:
            m = turn_messages(seed, by_id[e["thread_id"]], e["turn"])
            msgs = [{"role": "user", "text": m["user"]}]
            for tl in m["tools"]:
                msgs.append({"role": "assistant", "tool_call": {"id": tl["id"], "name": tl["name"], "args": tl["args"]}})
                msgs.append({"role": "tool", "tool_call_id": tl["id"], "text": tl["output"]})
            msgs.append({"role": "assistant", "text": m["answer"]})
            f.write(json.dumps(e | {"messages": msgs}) + "\n")


if __name__ == "__main__":
    main()
