#!/usr/bin/env python3
"""Splits a perf profile of the engine by layer (bench/perf, see docs/perf-letture.md).

Input: the text of `perf script` (call stacks, leaf first) with Rust names demangled (rustfilt), on stdin or as a file
(.gz accepted).
Every sample is given to ONE layer, looking at its whole call stack:
  - slatedb : a frame of slatedb, or of what only slatedb calls (foyer cache, object_store, the SST block codecs).
              Includes the malloc / memcpy / decompression done on its behalf;
  - mongodb : otherwise a frame of mongo_server or bson (command parsing, BSON, copies, cursors, the reply).
              Includes the malloc / memcpy done on its behalf;
  - tenant  : otherwise a frame of tenant_bench (the lifecycle wrapper around slatedb);
  - net     : otherwise socket / epoll / TCP in the kernel, or tokio's I/O driver (reading requests, writing replies);
  - runtime : otherwise tokio's scheduler, the allocator alone, thread parking;
  - other   : everything else.
A second table splits the mongodb layer by the first mongo_server function under it (leaf side), and a third lists
the leaf functions (self time) of each layer.

Usage: layers.py [perf-script.txt[.gz]] [--top N]
"""
import collections, gzip, re, sys

SLATEDB = re.compile(r"slatedb::|foyer_memory::|foyer::|object_store::")
MONGO = re.compile(r"mongo_server::|bson::")
TENANT = re.compile(r"tenant_bench::")
NET = re.compile(r"tcp_|sock_|__sys_sendto|__sys_recvfrom|ep_poll|epoll|tokio::io::|tokio::net::|mio::|do_syscall_64.*|__x64_sys_(read|write|sendto|recvfrom)|inet_|ip_|__netif|net_rx|loopback|skb")
RUNTIME = re.compile(r"tokio::runtime|tokio::sync|parking_lot|futex|schedule|finish_task_switch|__schedule|je_|jemalloc|_rjem_|malloc|free|sdallocx")


def samples(f):
    stack = []
    for line in f:
        line = line.rstrip("\n")
        if not line.strip():
            if stack:
                yield stack
            stack = []
            continue
        if line[0] not in " \t":
            stack = []  # header line of a sample
            continue
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            sym = parts[1]
            if parts[0].startswith("ffffffff"):
                sym = "[k] " + sym
            sym = re.sub(r"\+0x[0-9a-f]+", "", sym)
            sym = re.sub(r" \(.*\)$", "", sym)
            stack.append(sym)
    if stack:
        yield stack


def layer(stack):
    joined = "\n".join(stack)
    if SLATEDB.search(joined):
        return "slatedb"
    if MONGO.search(joined):
        return "mongodb"
    if TENANT.search(joined):
        return "tenant"
    if NET.search(joined):
        return "net"
    if RUNTIME.search(joined):
        return "runtime"
    return "other"


# What a sample of the SlateDB layer was doing for MongoDB: which mongo_server function asked for the read.
# (checked in this order, each against the whole stack: the first that matches names the read)
SLATE_CALLERS = [
    ("collection options read a second time (coll_kind)", "Server>::coll_kind"),
    ("collection list (view_lookup: is it a view?)", "Server>::view_lookup"),
    ("collection options (load_indexes)", "ReadView>::coll_opts"),
    ("collection options (load_indexes)", "Entry>::coll_opts"),
    ("index definitions (load_indexes)", "Server>::load_indexes"),
    ("collection list (update)", "Entry>::collections"),
    ("the document itself (find_docs)", "coll::find_docs"),
    ("writes (apply, stage, flush)", "Entry>::apply"),
    ("writes (apply, stage, flush)", "tenant_bench::lifecycle"),
    # background work, not attached to a command's stack
    ("compaction (background)", "compactor"),
    ("local object store I/O (WAL/SST files written, blocks read)", "object_store::local"),
    ("block fetches after a cache miss (background tasks)", "spawn_fetches"),
    ("block fetches after a cache miss (background tasks)", "RawFetch"),
    ("memtable flush / WAL (background)", "flush"),
]


def slate_caller(st):
    for name, pat in SLATE_CALLERS:
        if any(pat in s for s in st):
            return name
    return "other / stack cut"


def slate_kind(st):
    leaf = st[0]
    j = "\n".join(st[:12])
    if "malloc" in leaf or "sdallocx" in leaf or "rallocx" in leaf or "_rjem_" in leaf or "__rust_dealloc" in leaf:
        return "allocation (malloc/free)"
    if "memcpy" in leaf or "memmove" in leaf:
        return "memory copies"
    if "bytes::bytes::shared" in leaf:
        return "Bytes reference counts"
    if "snap::" in j or "zstd" in j or "lz4" in j or "decompress" in j:
        return "decompression"
    if "object_store::path" in j or "percent_encoding" in j or "PathResolver" in j or "TwoWaySearcher" in leaf:
        return "SST paths (format!, object_store::Path)"
    if "foyer" in j or "db_cache" in j:
        return "block / filter cache"
    if "filter" in leaf.lower() or "probes_for_key" in leaf or "bloom" in j.lower():
        return "bloom filters"
    if "[k]" in leaf:
        return "kernel (file reads)"
    if "iter" in leaf.lower() or "block" in leaf.lower() or "segment" in leaf or "sst" in leaf.lower():
        return "iterators and blocks"
    return "other"


def mongo_kind(st):
    leaf = st[0]
    if leaf.startswith("[k]"):
        return "kernel (socket read/write, wakeups)"
    if "malloc" in leaf or "sdallocx" in leaf or "rallocx" in leaf or "_rjem_" in leaf or "__rust_dealloc" in leaf:
        return "allocation (malloc/free)"
    if "memcpy" in leaf or "memmove" in leaf:
        nxt = st[1] if len(st) > 1 else ""
        if "bson" in nxt or "read_to_end" in nxt or "extend" in nxt or "Vec" in nxt:
            return "memory copies of data (BSON, buffers)"
        return "memory copies of async state (futures moved between wrappers)"
    if any(x.lstrip("<").startswith("bson::") for x in st[:3]):
        return "BSON encode/decode"
    return "command logic and the rest"


def short(sym):
    sym = re.sub(r"::h[0-9a-f]{16}$", "", sym)
    sym = re.sub(r"<[^<>]*>", "<>", sym) if len(sym) > 120 else sym
    return sym[:140]


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    top = 15
    if "--top" in sys.argv:
        top = int(sys.argv[sys.argv.index("--top") + 1])
    src = args[0] if args else None
    f = sys.stdin if not src else (gzip.open(src, "rt", errors="replace") if src.endswith(".gz") else open(src, errors="replace"))
    by_layer = collections.Counter()
    leaf = collections.defaultdict(collections.Counter)
    mongo_fn = collections.Counter()
    callers, skinds, mkinds = collections.Counter(), collections.Counter(), collections.Counter()
    n = 0
    for st in samples(f):
        n += 1
        l = layer(st)
        by_layer[l] += 1
        leaf[l][short(st[0])] += 1
        if l == "slatedb":
            callers[slate_caller(st)] += 1
            skinds[slate_kind(st)] += 1
        if l == "mongodb":
            mkinds[mongo_kind(st)] += 1
        if l == "mongodb":
            first = next((s for s in st if s.lstrip("<").startswith(("mongo_server::", "bson::"))), None)
            mongo_fn[short(first or "?")] += 1
    print(f"samples: {n}")
    print("\n# by layer (whole call stack)")
    for l, c in by_layer.most_common():
        print(f"{100.0 * c / n:6.1f}%  {l}")
    print("\n# slatedb layer: which MongoDB-layer read asked for it")
    for k, c in callers.most_common():
        print(f"{100.0 * c / n:6.1f}%  {k}")
    print("\n# slatedb layer: kind of work (leaf function)")
    for k, c in skinds.most_common():
        print(f"{100.0 * c / n:6.1f}%  {k}")
    print("\n# mongodb layer: kind of work")
    for k, c in mkinds.most_common():
        print(f"{100.0 * c / n:6.1f}%  {k}")
    print(f"\n# mongodb layer: first mongo_server frame from the leaf (top {top})")
    for s, c in mongo_fn.most_common(top):
        print(f"{100.0 * c / n:6.1f}%  {s}")
    for l, _ in by_layer.most_common():
        print(f"\n# {l}: leaf functions (top {top})")
        for s, c in leaf[l].most_common(top):
            print(f"{100.0 * c / n:6.1f}%  {s}")


if __name__ == "__main__":
    main()
