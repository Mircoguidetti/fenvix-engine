"""P-B3 step 1 (2026-11-13): the cook's compaction on the cook's CPU (as in the bench) or on a CPU of its own, with no
engine change. Used as the command of a mix round (lines.py run ... --rounds), before and after `lines.py restart`.

  b3sep.py prep            stop the pinner of the previous round, record the threads' CPU of the cook's process, set the
                           cook's quota back to 1 CPU (docker update), so the restart starts the server exactly as the bench
  b3sep.py shared          nothing more: the server pinned itself to one CPU of the quota at its start (pool::pin_to_quota);
                           every thread, the compaction's too (fx-bg, fx-wpath), shares that one CPU, as in the bench
  b3sep.py separate        the cook's quota becomes 2 CPUs; every thread of the compaction (fx-bg: merges, reseals,
                           erasures, GC, SlateDB's compactor and fold; fx-wpath: absorption and the fold the writes wait on)
                           moves to another CPU; every other thread (commands, SlateDB's flushers, the log's client) stays
                           on the server's CPU, so the commands still have exactly one CPU. A detached pinner moves new
                           compaction threads every 0.5 s until the next `prep`.

The server's own pacing (--compaction-cpu-pct, --compaction-window-pct) is the same in both modes: the process is the
same, started the same way; only where the compaction threads run differs.
Every action is logged with a timestamp to $B3_LOG (default /tmp/fxbench/b3sep.jsonl).
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time

LOG = os.environ.get("B3_LOG", "/tmp/fxbench/b3sep.jsonl")
PIDFILE = os.path.join(os.path.dirname(LOG), "b3sep.pinner.pid")
CG = "/sys/fs/cgroup"
BG_PREFIXES = ("fx-bg", "fx-wpath")


def log(**kw) -> None:
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    kw = {"t": round(time.time(), 3), **kw}
    with open(LOG, "a") as f:
        f.write(json.dumps(kw) + "\n")
    print(json.dumps(kw), flush=True)


def cook_pid() -> int | None:
    cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", "cook"], capture_output=True, text=True).stdout.strip()
    if not cid:
        return None
    for p in open(f"{CG}/cpuacct/docker/{cid}/cgroup.procs").read().split():
        try:
            if open(f"/proc/{p}/comm").read().strip() == "mongo-server":
                return int(p)
        except OSError:
            continue
    return None


def threads(pid: int) -> list[tuple[int, str]]:
    out = []
    for t in os.listdir(f"/proc/{pid}/task"):
        try:
            out.append((int(t), open(f"/proc/{pid}/task/{t}/comm").read().strip()))
        except OSError:
            continue
    return out


def thread_cpu_s(pid: int) -> dict:
    """CPU seconds (user+system) of the process's threads, compaction threads apart from the others."""
    tick = os.sysconf("SC_CLK_TCK")
    bg = other = 0.0
    for tid, comm in threads(pid):
        try:
            f = open(f"/proc/{pid}/task/{tid}/stat").read().rsplit(")", 1)[1].split()
        except OSError:
            continue
        s = (int(f[11]) + int(f[12])) / tick
        if comm.startswith(BG_PREFIXES):
            bg += s
        else:
            other += s
    return {"bg_s": round(bg, 2), "other_s": round(other, 2)}


def stop_pinner() -> None:
    try:
        pid = int(open(PIDFILE).read())
        os.kill(pid, signal.SIGTERM)
        log(action="pinner stopped", pid=pid)
    except (OSError, ValueError):
        pass
    try:
        os.remove(PIDFILE)
    except OSError:
        pass


def pin_once(pid: int, home: set, other: set) -> int:
    moved = 0
    for tid, comm in threads(pid):
        want = other if comm.startswith(BG_PREFIXES) else home
        try:
            if os.sched_getaffinity(tid) != want:
                os.sched_setaffinity(tid, want)
                moved += 1
        except OSError:
            continue
    return moved


def main() -> None:
    mode = sys.argv[1]
    if mode == "prep":
        stop_pinner()
        p = cook_pid()
        if p:
            log(action="prep", pid=p, **thread_cpu_s(p))
        subprocess.run(["docker", "update", "--cpus", "1", "cook"], check=True, capture_output=True)
        log(action="cook quota 1")
    elif mode == "shared":
        p = cook_pid()
        log(action="shared", pid=p, affinity=sorted(os.sched_getaffinity(p)), **thread_cpu_s(p))
    elif mode == "separate":
        p = cook_pid()
        home = os.sched_getaffinity(p)
        if len(home) != 1:
            raise SystemExit(f"the server is not pinned to one CPU: {sorted(home)}")
        cpu = next(iter(home))
        other = {(cpu + 2) % os.cpu_count()}
        subprocess.run(["docker", "update", "--cpus", "2", "cook"], check=True, capture_output=True)
        moved = pin_once(p, home, other)
        log(action="separate", pid=p, home=sorted(home), compaction_cpu=sorted(other), moved=moved, **thread_cpu_s(p))
        if os.fork() == 0:   # the detached pinner
            os.setsid()
            # the round's runner captures our output and waits for its pipes to close: the pinner keeps none of them
            nul = os.open(os.devnull, os.O_RDWR)
            for fd in (0, 1, 2):
                os.dup2(nul, fd)
            open(PIDFILE, "w").write(str(os.getpid()))
            n = 0
            while True:
                try:
                    n += pin_once(p, home, other)
                except OSError:
                    break
                time.sleep(0.5)
            os._exit(0)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
