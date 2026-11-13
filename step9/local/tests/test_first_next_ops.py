"""Coordinator's request of 2026-11-13: programs/first_next_ops.py (FIRST and NEXT of each request of the agent
benchmark: resume, last 10, whole history, time travel, save), first_next_compare.py (ours against another server) and
reads_after_writes.py (the stato_ts index read before and after updates). Logic with a fake server ($0, no database):
the real runs are the local docker round and the AWS pilot."""
import datetime as dt
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(S9, "programs"))
import first_next_compare as cmp_  # noqa: E402
import first_next_ops as ops  # noqa: E402
import reads_after_writes as raw  # noqa: E402


# ---------- a fake server: counts GETs the way a cook with (or without) the customer's box would ----------
class Cursor:
    def __init__(self, srv, coll, q):
        self.srv, self.coll, self.q, self.n, self.hinted = srv, coll, q, None, None

    def sort(self, s):
        return self

    def limit(self, n):
        self.n = n
        return self

    def max_time_ms(self, ms):
        self.srv.max_ms.append(ms)
        return self

    def hint(self, h):
        self.hinted = h
        return self

    def __iter__(self):
        self.srv.request(self.coll, self.q, self.n)
        return iter([{"_id": i, "ts": 0} for i in range(self.srv.docs(self.q, self.n))])

    def explain(self):
        return self.srv.explain(self.q, self.hinted)


class Coll:
    def __init__(self, srv, name):
        self.srv, self.name = srv, name

    def with_options(self, **kw):
        self.srv.write_concerns.append(kw["write_concern"].document)
        return self

    def find(self, q, proj=None):
        return Cursor(self.srv, self.name, q)

    def find_one(self, q, max_time_ms=None):
        self.srv.opened.append(q["cliente"])
        return {"_id": 0}

    def insert_one(self, doc):
        self.srv.request(self.name, {"cliente": doc["cliente"]}, 1, write=True)
        self.srv.inserted.append(doc)

    def update_one(self, q, u):
        self.srv.updates.append((q, u))


class Server:
    """box=True: a customer's first request makes 3 GETs, every later one 0. box=False: later reads make 2 GETs each."""
    def __init__(self, box=True, fail_op_on=None, fenvix=True, warm=()):
        self.box, self.fenvix, self.fail_on = box, fenvix, fail_op_on
        self.seen, self.get, self.bytes, self.rows = set(warm), 0, 0, 0
        self.inserted, self.updates, self.write_concerns, self.max_ms, self.log, self.opened = [], [], [], [], [], []
        self.admin = self
        self.amplified = False
        self.compactor = {"publishes": 1, "absorbs": 0}

    def __getitem__(self, db):
        srv = self

        class DB:
            def __getitem__(self, coll):
                return Coll(srv, coll)
        return DB()

    def list_database_names(self):
        return ["app_data_b01", "admin"]

    def docs(self, q, n):
        return min(n or 400, 400)

    def request(self, coll, q, n, write=False):
        cli = q.get("cliente")
        self.log.append((cli, coll, "write" if write else ("tt" if "ts" in q else n)))
        if self.fail_on and cli == self.fail_on:
            raise RuntimeError("boom")
        first = cli not in self.seen
        self.seen.add(cli)
        g = 3 if first else (0 if self.box else 2)
        self.get += g
        self.bytes += g * 1000
        self.rows += g * 10

    def explain(self, q, hinted):
        assert hinted == "stato_ts" and q["stato"] == "spedito"
        assert q["ts"]["$lt"] - q["ts"]["$gte"] == dt.timedelta(days=1)
        self.bytes += 1_600_000_000 if self.amplified else 50_000_000
        self.rows += 3_700_000 if self.amplified else 1_600
        self.get += 1
        return {"executionStats": {"totalDocsExamined": 800, "nReturned": 3}}

    def command(self, name):
        if not self.fenvix:
            if name == "serverStatus":
                return {"wiredTiger": {"cache": {"pages read into cache": self.get, "bytes read into cache": self.bytes}}}
            raise RuntimeError("no such command: tenantStats")
        if name == "tenantStats":
            return {"requests": {"get": self.get, "getBytes": self.bytes}, "maintenance": {"compactGets": 0, "gcGets": 0}}
        return {"fenvix": {"s3": {"getBytes": self.bytes, "getRange": self.get}, "sealedRuns": {"rowsFromRuns": self.rows},
                           "compactor": dict(self.compactor)}}


STATE = {"db": "app_data_b01", "clienti": {"nomi_da": "cli-%06d", "n": 400, "riservati": list(range(0, 400, 4))},
         "caricamento": {"byte_bson": 10 ** 12}}


def run(srv, per_op=6, label="before-writes"):
    g = ops.groups(STATE, per_op, 9, label)
    rows = []
    for i in range(per_op):
        for op in ops.OPS:
            ops.measure_customer(srv, "app_data_b01", g[op][i], op, 9, 120000, ops.pick_counters(srv, "auto")[1], rows, i)
    return g, rows, ops.summarize(rows, True)


def test_time_constants_are_the_frozen_scripts():
    sys.path.insert(0, os.path.join(S9, "..", "kitchen", "irlanda"))
    import prova_infinite as p
    assert ops.T_START == p.T_INIZIO and ops.T_SPAN_S == p.T_SPAN_S
    assert tuple(raw.STATES) == tuple(p.STATI)
    # the saved message has the fields of the script's own message
    assert set(p.doc_messaggio(__import__("random").Random(1), "c", ops.T_START)) <= set(ops.new_message("c", 0))


def test_groups_are_disjoint_never_reserved_fixed_by_seed_and_label():
    g = ops.groups(STATE, 10, 9, "before-writes")
    allc = [c for op in ops.OPS for c in g[op]]
    assert len(allc) == 50 and len(set(allc)) == 50
    assert all(int(c[4:]) % 4 != 0 for c in allc)                       # no reserved customer
    assert g == ops.groups(STATE, 10, 9, "before-writes")
    other = [c for v in ops.groups(STATE, 10, 9, "after-writes").values() for c in v]
    assert set(other) != set(allc)                                      # another label draws other customers
    assert sum(len(v) for v in ops.groups(None, 10, 9, "x", "d", 20).values()) == 20   # no more than the pool


def test_each_operation_is_first_for_its_group_and_next_for_the_others_save_last():
    srv = Server(box=True)
    g, rows, s = run(srv)
    for op in ops.OPS:
        for cli in g[op]:
            mine = [r for r in rows if r["cliente"] == cli]
            assert [r["op"] for r in mine] == ops.order_for(op)
            assert mine[0]["role"] == "first" and all(r["role"] == "next" for r in mine[1:])
            if op != "save":
                assert mine[-1]["op"] == "save"                        # the reads see the generated history only
    for op in ops.OPS:
        assert s["ops"][op]["first"]["n"] == 6 and s["ops"][op]["next"]["n"] == 24
    assert len(srv.inserted) == 30 and all(d["s9"].startswith("first-next-ops") for d in srv.inserted)
    assert all(wc == {"w": "majority"} for wc in srv.write_concerns)
    assert set(srv.max_ms) == {120000}


def test_with_the_box_every_next_read_makes_no_get_without_it_the_summary_says_so():
    _, _, s = run(Server(box=True))
    for op in ops.READS:
        assert s["ops"][op]["first"]["gets_p50"] == 3 and s["ops"][op]["next"]["gets_max"] == 0
        assert s["ops"][op]["next_gets_zero"] is True
    _, _, s = run(Server(box=False))
    for op in ops.READS:
        assert s["ops"][op]["next_gets_zero"] is False and s["ops"][op]["next"]["gets_p50"] == 2
    assert "NO, it still reads from outside" in ops.text({**s, "db": "d", "label": "l", "counters": "fenvix"})


def test_a_first_read_that_was_not_cold_is_left_out_and_counted():
    g = ops.groups(STATE, 6, 9, "before-writes")
    warm = {g["resume"][0], g["history"][1]}
    _, rows, s = run(Server(box=True, warm=warm))
    assert s["not_cold_at_first"] == 2 and s["cold_customers"] == 28
    assert s["ops"]["resume"]["first"]["n"] == 5 and s["ops"]["history"]["first"]["n"] == 5
    # their next requests are left out too: they are not "next after a cold first"
    assert s["ops"]["last10"]["next"]["n"] == 24 - 2


def test_a_failed_request_is_recorded_and_fails_the_run():
    g = ops.groups(STATE, 6, 9, "before-writes")
    _, rows, s = run(Server(fail_op_on=g["last10"][2]))
    assert s["errors"] == len(ops.order_for("last10"))


def test_counters_on_a_mongod_are_wiredtigers_and_none_when_asked():
    srv = Server(fenvix=False)
    name, fnc = ops.pick_counters(srv, "auto")
    assert name == "mongod" and fnc(srv) == {"gets": 0, "bytes": 0}
    rows = []
    ops.measure_customer(srv, "d", "cli-000001", "resume", 9, 1000, fnc, rows, 0)
    assert rows[0]["gets"] == 3 and "rows_from_runs" not in rows[0]


def test_main_end_to_end_with_a_fake_pymongo():
    import types
    srv = Server(box=True)
    fake = types.ModuleType("pymongo")
    fake.MongoClient = lambda *a, **k: srv
    wc = types.ModuleType("pymongo.write_concern")

    class WriteConcern:
        def __init__(self, w=None):
            self.document = {"w": w}
    wc.WriteConcern = WriteConcern
    saved = {k: sys.modules.get(k) for k in ("pymongo", "pymongo.write_concern")}
    sys.modules["pymongo"], sys.modules["pymongo.write_concern"] = fake, wc
    try:
        d = tempfile.mkdtemp()
        os.makedirs(os.path.join(d, "states", "app_data_b01"))
        json.dump(STATE, open(os.path.join(d, "states", "app_data_b01", "risultati_p1.json"), "w"))
        rc = ops.main([os.path.join(d, "out"), "--uri", "mongodb://x", "--states", os.path.join(d, "states"), "--per-op", "4"])
        s = json.load(open(os.path.join(d, "out", "first-next-ops-summary.json")))
        assert rc == 0 and s["ok"] and s["db"] == "app_data_b01" and s["requests"] == 4 * 5 * 5 and s["counters"] == "fenvix"
        # the opening read: one customer outside every group, before the measure, kept apart
        assert len(srv.opened) == 1 and s["opening"]["cliente"] == srv.opened[0] and s["opening"]["error"] is None
        measured = {json.loads(l)["cliente"] for l in open(os.path.join(d, "out", "first-next-ops-requests.jsonl"))}
        assert srv.opened[0] not in measured and int(srv.opened[0][4:]) % 4 != 0
        # --no-save: the four reads only, nothing written, no save line
        before = len(srv.inserted)
        rc = ops.main([os.path.join(d, "out3"), "--uri", "mongodb://x", "--states", os.path.join(d, "states"), "--per-op", "4",
                       "--no-save", "--label", "pilot"])
        s3 = json.load(open(os.path.join(d, "out3", "first-next-ops-summary.json")))
        assert rc == 0 and len(srv.inserted) == before and "save" not in s3["ops"] and s3["requests"] == 4 * 4 * 4
        assert s3["saves"] is False and "save" not in s3["text"]
        # no database on the server: fails, never an empty success
        srv.list_database_names = lambda: ["admin"]
        assert ops.main([os.path.join(d, "out2"), "--uri", "mongodb://x", "--states", os.path.join(d, "states")]) == 1
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


# ---------- the comparison ----------
def _summary(first, nxt, **kw):
    line = lambda p50, p99: {"n": 30, "p50_ms": p50, "p99_ms": p99}   # noqa: E731
    return {"db": "d", "label": "before-writes", "seed": 9, "per_op": 30, **kw,
            "ops": {op: {"first": line(*first), "next": line(*nxt)} for op in ops.OPS}}


def test_compare_marks_within_and_over_the_20_ms_tolerance_and_the_ratio():
    c = cmp_.compare(_summary((60.0, 200.0), (4.0, 22.0)), _summary((5.0, 30.0), (2.0, 5.0)))
    n = c["ops"]["resume"]["next"]
    assert n["p50_ms"]["tolerance"] == "WITHIN" and n["p50_ms"]["ratio_verdict"] == "SLOWER" and n["p50_ms"]["ratio"] == 2.0
    assert n["p99_ms"]["tolerance"] == "WITHIN" and n["p99_ms"]["ours_minus_theirs_ms"] == 17.0
    assert c["ops"]["resume"]["first"]["p50_ms"]["tolerance"] == "OVER"    # reported; FIRST does not decide
    assert c["every_next_within_tolerance"] is True and c["same_plan"] is True
    c = cmp_.compare(_summary((60.0, 200.0), (4.0, 26.0)), _summary((5.0, 30.0), (2.0, 5.0)))
    assert c["every_next_within_tolerance"] is False
    assert "NOT THE SAME PLAN" in cmp_.text(cmp_.compare(_summary((1, 1), (1, 1), seed=1), _summary((1, 1), (1, 1))), "mongod")
    assert "[FIRST: reported, not judged]" in cmp_.text(c, "mongod")


# ---------- reads after writes ----------
def test_reads_after_writes_shows_the_amplification_and_the_writes():
    srv = Server()
    names = [f"cli-{i:06d}" for i in range(400)]
    reads = raw.the_reads(names, 4, 5)
    assert reads == raw.the_reads(names, 4, 5)                           # the same reads at every stage
    rows = []
    raw.stage(srv, srv, "app_data_b01", reads, "before", rows)
    w = raw.writes(srv, "app_data_b01", names, 20, 300, 5)
    assert w["updates"] == 300 and w["n_errors"] == 0 and w["update_p99_ms"] is not None
    q, u = srv.updates[0]
    assert set(q) == {"cliente", "_id"} and u["$inc"] == {"totale": 1.5} and u["$set"]["stato"] in raw.STATES
    srv.amplified = True
    raw.stage(srv, srv, "app_data_b01", reads, "after-writes", rows)
    raw.stage(srv, srv, "app_data_b01", reads, "settled", rows)
    s = raw.summarize(rows)
    assert s["before"]["rows_per_doc_examined_p50"] == 2.0
    assert s["after-writes"]["rows_per_doc_examined_p50"] == 4625.0
    assert s["after-writes"]["rows_per_doc_times_before"] == 2312.5 and s["after-writes"]["mb_fetched_p50"] == 1600.0
    t = raw.text({"db": "d", "reads_per_stage": 4, "writes": w, "settle": {}, "stages": s})
    assert "rows per document x2312.5" in t


def test_a_timed_out_read_is_a_result_and_stops_its_stage():
    srv = Server()

    class Slow(Server):
        def explain(self, q, hinted):
            raise TimeoutError("timed out")
    slow = Slow()
    rows = []
    raw.stage(slow, srv, "d", raw.the_reads(["cli-000001", "cli-000002"], 5, 5), "after-writes", rows)
    assert len(rows) == 1 and rows[0]["timed_out"] and rows[0]["error"] is None
    s = raw.summarize(rows)
    assert s["after-writes"] == {"reads": 1, "answered": 0, "timed_out": 1, "errors": 0}


def test_settle_waits_for_a_quiet_compactor_and_says_when_it_never_is():
    srv = Server()
    clock = {"t": 0.0}

    def sleep(s):
        clock["t"] += s
        if clock["t"] <= 30:
            srv.compactor["absorbs"] += 1          # busy for 30 s, then quiet
    st = raw.settle(srv, 20, quiet_s=60, poll_s=5, sleep=sleep, now=lambda: clock["t"])
    assert st["quiet"] is True and st["changed"] is True and 90 <= st["waited_s"] <= 100
    clock["t"] = 0.0

    def busy(s):
        clock["t"] += s
        srv.compactor["absorbs"] += 1
    st = raw.settle(srv, 2, quiet_s=60, poll_s=5, sleep=busy, now=lambda: clock["t"])
    assert st["quiet"] is False and st["waited_s"] >= 120
    assert raw.settle(Server(fenvix=False), 1)["quiet"] is None           # another server: no counters, no wait


if __name__ == "__main__":
    for k, v in list(globals().items()):
        if k.startswith("test_"):
            v()
    print("ALL PASS")
