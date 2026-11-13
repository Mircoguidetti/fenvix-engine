"""A memory-limited cgroup for one server process (review of big-speed, 2026-11-13: the reviewer's 1 GiB cgroup).

The group is a child of this test process's own memory cgroup, without its own namespace: exactly the set-up of the
review's error 3 (a supervisor's limit on a group deeper than the mount's root). The kernel enforces the limit by
killing the process (as Docker's `--memory 1g` does); `oom_kills()` reads the group's counter.

cgroup v1 (the sandbox, Docker on cgroup v1): `memory.limit_in_bytes`, `memory.max_usage_in_bytes`, `memory.oom_control`.
cgroup v2 (GitHub's runners): `memory.max`, `memory.peak`, `memory.events`; the parent must let the child use the memory
controller (`cgroup.subtree_control`), which needs root. When neither can be set up the caller gets `Unavailable` and
the test fails with that reason (it is never counted as a pass).
"""
import os


class Unavailable(Exception):
    pass


def _own(kind):
    for line in open("/proc/self/cgroup").read().splitlines():
        hid, ctrls, path = line.split(":", 2)
        if kind == 1 and "memory" in ctrls.split(","):
            return path
        if kind == 2 and hid == "0" and ctrls == "":
            return path
    return None


def _mount(kind):
    for line in open("/proc/self/mountinfo").read().splitlines():
        left, right = line.split(" - ", 1)
        l, r = left.split(), right.split()
        if kind == 1 and r[0] == "cgroup" and "memory" in r[2].split(","):
            return l[3], l[4]
        if kind == 2 and r[0] == "cgroup2":
            return l[3], l[4]
    return None


class MemCg:
    def __init__(self, name, limit_bytes):
        self.v, self.dir = None, None
        errs = []
        for kind in (1, 2):
            own, m = _own(kind), _mount(kind)
            if own is None or m is None:
                errs.append(f"v{kind}: no own group or mount")
                continue
            root, point = m
            rel = own[len(root.rstrip("/")):] if own.startswith(root.rstrip("/")) else own
            parent = point.rstrip("/") + rel.rstrip("/")
            d = os.path.join(parent, name)
            try:
                os.makedirs(d, exist_ok=True)
                if kind == 1:
                    open(os.path.join(d, "memory.limit_in_bytes"), "w").write(str(limit_bytes))
                    open(os.path.join(d, "memory.max_usage_in_bytes"), "w").write("0")
                else:
                    try:
                        open(os.path.join(parent, "cgroup.subtree_control"), "w").write("+memory")
                    except OSError:
                        pass
                    open(os.path.join(d, "memory.max"), "w").write(str(limit_bytes))
                    try:
                        open(os.path.join(d, "memory.swap.max"), "w").write("0")
                    except OSError:
                        pass
                self.v, self.dir = kind, d
                break
            except OSError as e:
                errs.append(f"v{kind} at {d}: {e}")
        if self.v is None:
            raise Unavailable("; ".join(errs))
        self.oom0 = self.oom_kills()

    def add(self, pid):
        open(os.path.join(self.dir, "cgroup.procs"), "w").write(str(pid))

    def oom_kills(self):
        f = "memory.oom_control" if self.v == 1 else "memory.events"
        for line in open(os.path.join(self.dir, f)).read().splitlines():
            if line.startswith("oom_kill "):
                return int(line.split()[1])
        return 0

    def new_oom_kills(self):
        return self.oom_kills() - self.oom0

    def peak_mib(self):
        f = "memory.max_usage_in_bytes" if self.v == 1 else "memory.peak"
        try:
            return int(open(os.path.join(self.dir, f)).read()) >> 20
        except OSError:
            return -1


def rss_mib(pid):
    try:
        return int(open(f"/proc/{pid}/status").read().split("VmRSS:")[1].split()[0]) // 1024
    except Exception:
        return -1


def wrapper(cg, binary=None):
    """A launcher that joins `cg` before it runs the server (`exec`): the server reads its memory limit at start, so it
    must already be inside the group (moving it in after `Popen` races with that read)."""
    from lib import BIN, OUT
    binary = binary or BIN
    p = os.path.join(OUT, f"in-cgroup-{os.path.basename(cg.dir)}.sh")
    with open(p, "w") as f:
        f.write(f"#!/bin/sh\necho $$ > '{cg.dir}/cgroup.procs' || exit 97\nexec '{binary}' \"$@\"\n")
    os.chmod(p, 0o755)
    return p
