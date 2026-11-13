"""Attribute each sample to the leaf-most frame in our code (mongo-server/src, src/fx, src/*.rs of tenant_bench),
then print the top call sites with what they call (the leaf frame's family)."""
import collections, re, sys
f = sys.argv[1]; top = int(sys.argv[2]) if len(sys.argv) > 2 else 40
OURS = re.compile(r"@(src|fx|ctl|diag|expr)/[a-z0-9_]+\.rs:\d+$")
# frames of our crates have file names like src/coll.rs; std/alloc/tokio have other dirs; slatedb also uses src/...
SLATE = re.compile(r"slatedb|foyer|object_store|tokio|bson::|futures|hyper|reqwest|core::|alloc::|std::")
tot = 0; site = collections.Counter(); leafkind = collections.defaultdict(collections.Counter)
for line in open(f):
    st, _, n = line.rstrip("\n").rpartition(" "); n = int(n); tot += n
    fr = st.split(";")
    ours = None
    for x in reversed(fr):
        if ("mongo_server" in x or "tenant_bench" in x) and "@" in x:
            ours = x; break
    if ours is None:
        for x in reversed(fr):
            if OURS.search(x) and not SLATE.search(x.split("@")[0]):
                ours = x; break
    key = re.sub(r"::h[0-9a-f]{16}", "", ours or "?")
    key = key[-150:]
    site[key] += n
    leaf = fr[-1].split("@")[0]
    leafkind[key][leaf[:60]] += n
print("total", tot)
for k, v in site.most_common(top):
    lk = ", ".join(f"{a} {100*b/v:.0f}%" for a, b in leafkind[k].most_common(3))
    print(f"{100*v/tot:6.2f}%  {k}\n          leaves: {lk}")
