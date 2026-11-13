"""Per-layer split of a folded profile (cpu-prof): each sample goes to the first layer (in this order) one of whose
frames is on its stack. Layers follow the prompt's list: wire decode, auth, command dispatch, plan, merged read of runs
and SlateDB, decode of documents (incl. derived), BSON encode of the reply, the reply's send, background work."""
import collections, re, sys
LAYERS = [
    ("background (compactor, flush, GC)", r"fx::compactor|fx::drop|fx::gc|fx-bg|fx-wpath|fx-flush|sst_builder|compactor_executor|SSTGC|locallog::.*upload"),
    ("reply send (syscall)", r"tls::Stream>@io/split.rs:126|__send|__libc_send|writev"),
    ("reply BSON encode", r"write_reply|wire\.rs:(26[0-9]|27[0-9]|28[0-9]|29[0-9]|4[0-4][0-9])|encode_doc_into|enc_doc|enc_value|to_vec<bson|RawDocumentBuf"),
    ("wire decode (request)", r"wire\.rs:1[0-9][0-9]|read_request|parse_msg|read_doc|__recv|__libc_recv"),
    ("auth", r"auth::|authz::"),
    ("merged read: SlateDB scan setup + iteration", r"scan_with_options|segment_iterator|merge_iterator|db_iter|build_l0|build_sr|filter_iterator"),
    ("merged read: SlateDB point get", r"get_key_value_with_options|might_contain|fetch_filter|db_reader::DbReader>@src/future.rs|slatedb::db::Db>@src/future.rs|read::get::"),
    ("merged read: our runs (cursor, point get, fence)", r"fx::view|runfmt|fx::counter|fx::read::scan|fx::floor|warm_range|custbox|fx::derive"),
    ("merged read: other (layout, scan glue)", r"layout2::|scan_range|get_many|lifecycle::"),
    ("document decode", r"coll\.rs:(343|353|392|42[0-9]|44[0-9]|46[0-9])|Document::from_reader|coll::decode|from_reader"),
    ("plan + index catalog", r"index::plan|bounds_for|plan_for|IndexDef|cmd_index\.rs:3[67][0-9]|load_indexes|resolve_hint"),
    ("command dispatch + per-command bookkeeping", r"cmd::|cmd_data::|views::|fair::|diag::|meter::|collation::|coll::find|query::|handle_connection|dispatch"),
    ("runtime, epoll, allocator, other", r"."),
]
f = sys.argv[1]
tot = 0; c = collections.Counter()
rx = [(n, re.compile(r)) for n, r in LAYERS[:-1]]
for line in open(f):
    st, _, n = line.rstrip("\n").rpartition(" "); n = int(n); tot += n
    got = LAYERS[-1][0]
    # the leaf-most frame that belongs to a layer decides (a send under the reply path is "send", a memcpy under the
    # document decode is "decode", and so on)
    for fr in reversed(st.split(";")):
        hit = next((name for name, r in rx if r.search(fr)), None)
        if hit:
            got = hit
            break
    c[got] += n
print(f"{f.split('/')[-1]}: {tot} samples")
for name, _ in LAYERS:
    print(f"  {100*c[name]/tot:5.1f}%  {name}")
