"""Summaries of a folded-stack profile (cpu-prof build): per thread group, top self and inclusive frames, and per layer.

  python fold.py FILE [--top 40] [--grep REGEX] [--layers]
"""
import argparse
import collections
import re

ap = argparse.ArgumentParser()
ap.add_argument("file")
ap.add_argument("--top", type=int, default=40)
ap.add_argument("--grep", default=None, help="only stacks that contain a frame matching this")
ap.add_argument("--layers", action="store_true")
ap.add_argument("--under", default=None, help="inclusive/self tables restricted to the part of stacks below this frame")
a = ap.parse_args()

# Layers, first match from the leaf up wins (order matters: most specific first)
LAYERS = [
    ("alloc", r"^(_rjem_|je_|tikv_jemalloc|__rust_alloc|__rust_dealloc|__rust_realloc|alloc::alloc|<alloc::alloc::Global|malloc|free|realloc|calloc|__libc_malloc|cfree)"),
    ("memcpy", r"^(__memmove|__memcpy|memcpy|memmove|__memset|memset)"),
    ("bson encode", r"(bson::ser|bson::raw::document_buf|RawDocumentBuf::|to_vec|Document::to_writer|bson::.*serialize|encode_document|serialize_bson)"),
    ("bson decode", r"(bson::de|RawDocument::|Document::from_reader|from_slice|bson::raw::iter|deserialize_bson|bson::.*Deserial)"),
    ("wire", r"mongo_server::wire"),
    ("lock", r"(parking_lot|std::sync::.*Mutex|futex|tokio::sync::(mutex|rwlock|batch_semaphore)|RwLock)"),
    ("slatedb", r"^slatedb::"),
    ("fx read (runs merge)", r"tenant_bench::fx::(read|view|codec|counter|derive|seqfilter|pair|local)"),
    ("tokio runtime", r"^tokio::runtime"),
    ("syscall/kernel", r"(__libc_|syscall|epoll|writev|recv|send|__GI_|__lll_|clock_gettime|__vdso)"),
]


def load():
    for line in open(a.file):
        line = line.rstrip("\n")
        if not line:
            continue
        st, _, n = line.rpartition(" ")
        fr = st.split(";")
        yield fr, int(n)


def norm(f):
    f = re.sub(r"::h[0-9a-f]{16}$", "", f)
    f = re.sub(r"\{\{closure\}\}", "{c}", f)
    return f


stacks = []
for fr, n in load():
    fr = [norm(x) for x in fr]
    if a.grep and not any(re.search(a.grep, x) for x in fr):
        continue
    if a.under:
        idx = next((i for i, x in enumerate(fr) if re.search(a.under, x)), None)
        if idx is None:
            continue
        fr = fr[:1] + fr[idx:]
    stacks.append((fr, n))
tot = sum(n for _, n in stacks)
thr = collections.Counter()
for fr, n in stacks:
    thr[re.sub(r"[-_ ]?\d+$", "", fr[0])] += n
print(f"total samples {tot}")
print("threads:", ", ".join(f"{k} {100*v/tot:.1f}%" for k, v in thr.most_common(12)))
selfc, incl = collections.Counter(), collections.Counter()
for fr, n in stacks:
    selfc[fr[-1]] += n
    for x in set(fr[1:]):
        incl[x] += n
print(f"\n-- self top {a.top}")
for k, v in selfc.most_common(a.top):
    print(f"{100*v/tot:6.2f}%  {k[:200]}")
print(f"\n-- inclusive top {a.top} (mongo_server / tenant_bench / slatedb / bson frames only)")
for k, v in [(k, v) for k, v in incl.most_common(4000) if re.search(r"(mongo_server|tenant_bench|slatedb|bson|foyer|mongo-server/src|/src/fx|src/lifecycle)", k)][: a.top]:
    print(f"{100*v/tot:6.2f}%  {k[:200]}")
if a.layers:
    lay = collections.Counter()
    for fr, n in stacks:
        got = "other"
        for f in reversed(fr[1:]):
            for name, rx in LAYERS:
                if re.search(rx, f):
                    got = name
                    break
            if got != "other":
                break
        lay[got] += n
    print("\n-- layers (leaf-most matching frame)")
    for k, v in lay.most_common():
        print(f"{100*v/tot:6.2f}%  {k}")
