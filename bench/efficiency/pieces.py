#!/usr/bin/env python3
"""Splits a perf profile of mongo-server into the pieces of the E1 study (README.md).

Input: `perf script` text with call stacks (leaf first), plain or .gz, on stdin or as a file.
Every sample goes to exactly one piece, so the pieces add up to 100% of the server's CPU samples.

View 1 (flat, the list of the study): first the cross-cutting kinds of the LEAF frame
    alloc   : the allocator (jemalloc, Rust alloc/realloc/free, Vec growth) and kernel page faults;
    memcpy  : user-space memcpy / memmove / memset;
    lock    : futexes, parking_lot, std/tokio mutexes and semaphores, spin loops;
  then, for the rest, the piece that owns the stack:
    slatedb.compaction : compaction and garbage collection tasks (any frame of compactor / gc);
    slatedb.flush_l0   : memtable -> L0 SST flush;
    slatedb.wal        : the current log ("quaderno attuale": slatedb's WAL buffer and WAL flush);
    store.files        : the local object store (stand-in for S3): file create/write/fsync/rename in the kernel and
                         object_store::local, on blocking threads that do not show who asked for the write;
    slatedb.memtable   : the write path into the memtable (WriteBatch, skiplist inserts);
    slatedb.read       : reads (get / scan, SST and block iterators, block cache, bloom filters);
    mongo.protocol     : reading and writing wire messages (mongo_server::wire, kernel TCP, epoll, tokio I/O);
    mongo.bson         : BSON decode / encode / clone (bson::, Document, indexmap, coll::decode);
    mongo.index        : index keys (IndexDef::entries, keyenc, entry keys), duplicate checks on unique indexes;
    mongo.plan         : query planning and matching (index::plan, coll::plan_for, query::, find_docs);
    mongo.command      : everything else of mongo_server (command dispatch, metadata, fair queue, retry, cursors);
    runtime            : tokio scheduler, thread switches;
    other              : anything else.
View 2: the same, with alloc / memcpy / lock given back to the piece that owns their stack.

Callers: for every piece, the nearest mongo_server function on the stack (leaf side) that is not a closure of a
generic helper, so that e.g. slatedb.read can be split into "Writes::exists" (the _id duplicate check), "find", ...

Usage: pieces.py PERF_SCRIPT[.gz] [--top N] [--json OUT.json] [--callers N]
"""
import collections, gzip, json, re, sys

LEAF_ALLOC = re.compile(r"_rjem_|\bje_|jemalloc|\bmalloc\b|\bcalloc\b|\brealloc\b|\bfree\b|sdallocx|__rust_alloc|__rust_dealloc|"
                        r"__rust_realloc|__rdl_|finish_grow|RawVec.*(grow|reserve)|do_reserve_and_handle|"
                        r"asm_exc_page_fault|exc_page_fault|do_anonymous_page|clear_page|handle_mm_fault|do_user_addr_fault|"
                        r"__handle_mm_fault|alloc_pages|get_page_from_freelist|__free_pages|free_unref|zap_pte|unmap_page|"
                        r"madvise|__x64_sys_madvise|do_madvise|tlb_finish_mmu|release_pages")
LEAF_MEMCPY = re.compile(r"__memmove|__memcpy|\bmemcpy\b|\bmemmove\b|__memset|\bmemset\b|core::slice::.*copy_from_slice")
LEAF_LOCK = re.compile(r"futex|parking_lot|std::sys::sync|lock_contended|::mutex::|Mutex.*lock|RwLock|batch_semaphore|"
                       r"spin_loop|lll_lock|pthread_mutex|crossbeam_utils::backoff")

CTX = [  # (piece, regex over the whole stack) checked in this order
    ("slatedb.compaction", re.compile(r"slatedb::compactor|slatedb::compaction|slatedb::subcompaction|slatedb::size_tiered|"
                                      r"slatedb::garbage_collector|tenant_bench::lifecycle::[^\n]*(compact|gc|sweep)|"
                                      r"tenant_bench::warm")),
    ("slatedb.flush_l0", re.compile(r"slatedb::memtable_flusher|slatedb::flush::|flush_imm|MemtableFlusher|"
                                    r"slatedb::db::DbInner::flush_memtable|l0_flush|write_l0")),
    ("slatedb.wal", re.compile(r"slatedb::wal|WalBuffer|wal_buffer|flush_wal|WalFlush")),
    ("store.files", re.compile(r"object_store::local|\[k\] (ext4_|jbd2|vfs_write|vfs_fsync|do_fsync|__x64_sys_fsync|"
                               r"__x64_sys_fdatasync|__x64_sys_openat|__x64_sys_rename|do_renameat|__x64_sys_unlink|"
                               r"__x64_sys_pwrite|__x64_sys_write\b|ksys_write|filemap_|iomap_|blk_|virtio_blk|"
                               r"__x64_sys_pread|__x64_sys_read\b.*file|path_openat|do_sys_open|__x64_sys_statx|"
                               r"__x64_sys_newfstatat|__x64_sys_getdents|__x64_sys_mkdir)")),
    ("slatedb.memtable", re.compile(r"slatedb::mem_table|crossbeam_skiplist|slatedb::batch_write|slatedb::batch::|"
                                    r"WriteBatch|slatedb::db::DbInner::write|slatedb::db::Db::write|slatedb::oracle|"
                                    r"slatedb::seq_tracker")),
    ("slatedb.read", re.compile(r"slatedb::|foyer|object_store::|tenant_bench::")),
]
MONGO_LEAFMOST = [  # first frame from the leaf that matches decides
    ("mongo.plan", re.compile(r"mongo_server::index::plan|mongo_server::coll::plan_for|mongo_server::query::|"
                              r"mongo_server::coll::find_docs|mongo_server::projfind|mongo_server::find_ext|mongo_server::expr")),
    ("mongo.index", re.compile(r"mongo_server::index::|mongo_server::keyenc|encode_sortable|entry_key|mongo_server::coll::doc_key|"
                               r"mongo_server::collation")),
    ("mongo.bson", re.compile(r"\bbson::|indexmap::|mongo_server::coll::decode|<bson::|serde")),
    ("mongo.protocol", re.compile(r"mongo_server::wire|mongo_server::netguard|mongo_server::tls")),
]
NET = re.compile(r"\[k\] (tcp_|sock_|inet_|ip_|__sys_sendto|__sys_recvfrom|__x64_sys_sendto|__x64_sys_recvfrom|ep_poll|"
                 r"do_epoll|__x64_sys_epoll|__netif|net_rx|loopback|__dev_queue|skb_|__tcp|sk_)|tokio::io::|tokio::net::|mio::")
RUNTIME = re.compile(r"tokio::runtime|\[k\] (__schedule|schedule\b|finish_task_switch|pick_next|__switch_to|"
                     r"do_nanosleep|hrtimer|try_to_wake_up|ttwu_|sched_|enqueue_task|dequeue_task)")


def samples(f):
    stack, comm = [], ""
    for line in f:
        line = line.rstrip("\n")
        if not line.strip():
            if stack:
                yield comm, stack
            stack = []
            continue
        if line[0] not in " \t":
            stack = []
            comm = line.split()[0] if line.split() else ""
            continue
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            sym = parts[1]
            if parts[0].startswith("ffffffff"):
                sym = "[k] " + sym
            sym = re.sub(r"\+0x[0-9a-f]+", "", sym)
            sym = re.sub(r" \((/|\[)[^)]*\)$", "", sym)
            stack.append(sym)
    if stack:
        yield comm, stack


def owner(stack):
    joined = "\n".join(stack)
    for name, rx in CTX:
        if rx.search(joined):
            return name
    if "mongo_server::" in joined or "bson::" in joined:
        for fr in stack:
            for name, rx in MONGO_LEAFMOST:
                if rx.search(fr):
                    return name
            if fr.startswith("mongo_server::") or "<mongo_server::" in fr:
                return "mongo.command"
        return "mongo.command"
    if NET.search(joined):
        return "mongo.protocol"
    if RUNTIME.search(joined):
        return "runtime"
    return "other"


CALLER_SKIP = re.compile(r"mongo_server::(error|fair|dbs::Entry>::(get_live|get|scan)|lease|retry|meter)")


def caller(stack):
    for fr in stack:
        if "mongo_server::" in fr and not CALLER_SKIP.search(fr):
            name = re.sub(r"::\{closure#\d+\}", "", fr)
            name = re.sub(r"<([^<>]*) as [^<>]*>", r"\1", name)
            return name[:110]
    return "(no mongo_server frame)"


def leaf_kind(stack):
    leaf = stack[0]
    if LEAF_MEMCPY.search(leaf):
        return "memcpy"
    if LEAF_ALLOC.search(leaf):
        return "alloc"
    if LEAF_LOCK.search(leaf) or any(re.search(r"\[k\] (futex_|do_futex|__x64_sys_futex)", fr) for fr in stack[:12]):
        return "lock"
    return None


def main():
    args = sys.argv[1:]
    top, out_json, path, ncallers = 15, None, None, 8
    while args:
        a = args.pop(0)
        if a == "--top":
            top = int(args.pop(0))
        elif a == "--callers":
            ncallers = int(args.pop(0))
        elif a == "--json":
            out_json = args.pop(0)
        else:
            path = a
    f = (gzip.open(path, "rt", errors="replace") if path and path.endswith(".gz")
         else open(path, errors="replace") if path else sys.stdin)
    flat, full = collections.Counter(), collections.Counter()
    cross_owner = collections.defaultdict(collections.Counter)
    leaves = collections.defaultdict(collections.Counter)
    threads = collections.Counter()
    callers = collections.defaultdict(collections.Counter)
    n = 0
    for comm, st in samples(f):
        n += 1
        threads[comm] += 1
        own = owner(st)
        kind = leaf_kind(st)
        piece = kind or own
        flat[piece] += 1
        full[own] += 1
        if kind:
            cross_owner[kind][own] += 1
        leaves[piece][st[0]] += 1
        callers[own][caller(st)] += 1
    if not n:
        print("no samples")
        return
    pct = lambda c: 100.0 * c / n
    print(f"samples: {n}")
    print("\nView 1 (flat):")
    for k, c in flat.most_common():
        extra = ""
        if k in cross_owner:
            extra = "   (from: " + ", ".join(f"{o} {pct(x):.1f}" for o, x in cross_owner[k].most_common(4)) + ")"
        print(f"  {k:20s} {pct(c):5.1f}%{extra}")
    print(f"  {'SUM':20s} {pct(sum(flat.values())):5.1f}%")
    print("\nView 2 (alloc, memcpy, lock given back to their owner):")
    for k, c in full.most_common():
        print(f"  {k:20s} {pct(c):5.1f}%")
    print("\nThreads (perf comm):")
    for k, c in threads.most_common(10):
        print(f"  {k:24s} {pct(c):5.1f}%")
    print(f"\nCallers (nearest mongo_server function) per piece of view 2, % of all samples:")
    for k, _ in full.most_common():
        print(f"  [{k}]")
        for fn, c in callers[k].most_common(ncallers):
            print(f"     {pct(c):5.2f}%  {fn}")
    print(f"\nTop {top} leaf functions per piece (self time, % of all samples):")
    for k, _ in flat.most_common():
        print(f"  [{k}]")
        for fn, c in leaves[k].most_common(top):
            print(f"     {pct(c):5.2f}%  {fn[:150]}")
    if out_json:
        json.dump({"samples": n, "flat": {k: pct(c) for k, c in flat.items()},
                   "full": {k: pct(c) for k, c in full.items()},
                   "cross_owner": {k: {o: pct(x) for o, x in v.items()} for k, v in cross_owner.items()}},
                  open(out_json, "w"), indent=1)


if __name__ == "__main__":
    main()
