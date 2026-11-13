"""Total review R1 point 2 (2026-11-12): programs/first_next.py, the first request of a cold customer and three
different next requests of the same customer, with the cook's GETs and bytes per request. Logic with a fake client
($0); the run on our real server is in step9/local/first-next-local-27014ff/."""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(S9, "programs"))
import first_next as fn  # noqa: E402


def test_customers_are_never_the_scripts_reserved_ones_and_are_fixed_by_the_seed():
    st = {"db": "d", "clienti": {"nomi_da": "cli-%06d", "n": 50, "riservati": list(range(0, 50, 2))}}
    a = fn.customers(st, 10, 9)
    assert a == fn.customers(st, 10, 9) and len(set(a)) == 10
    assert all(int(c[4:]) % 2 == 1 for c in a), a          # every reserved (even) customer left out
    assert fn.customers(st, 100, 9) and len(fn.customers(st, 100, 9)) == 25   # at most the pool


class Coll:
    def __init__(self, log, name):
        self.log, self.name = log, name

    def find(self, q):
        self.log.append(("find", self.name, json.dumps(q, default=str)))
        return self

    def sort(self, s):
        return self

    def limit(self, n):
        return [{"ts": i} for i in range(min(n, 5))]

    def aggregate(self, p):
        self.log.append(("aggregate", self.name, p[0]["$match"]["cliente"]))
        return [{"_id": "a"}]


class Client:
    """tenantStats: the GETs grow by plan[kind] on each request; the first request of `warm` customers makes none."""
    def __init__(self, plan, warm=()):
        self.log, self.get, self.bytes, self.calls, self.plan, self.warm = [], 0, 0, 0, plan, set(warm)
        self.admin = self
        self.cur = None

    def command(self, name):
        assert name == "tenantStats"
        return {"requests": {"get": self.get, "getBytes": self.bytes}, "maintenance": {"compactGets": 0, "gcGets": 0}}

    def __getitem__(self, db):
        client = self

        class DB:
            def __getitem__(self, coll):
                return Coll(client.log, coll)
        return DB()


def run_with(plan, warm=(), n=4):
    c = Client(plan, warm)
    rows = []
    for cli in [f"cli-{i:06d}" for i in range(n)]:
        orig = fn.requests_of

        def wrapped(db, cli=cli):
            out = []
            for kind, f in orig(db, cli):
                def g(f=f, kind=kind):
                    r = f()
                    gets = 0 if (kind == "first" and cli in c.warm) else plan[kind]
                    c.get += gets
                    c.bytes += gets * 1000
                    return r
                out.append((kind, g))
            return out
        fn.requests_of = wrapped
        try:
            fn.measure(c, "d", cli, rows)
        finally:
            fn.requests_of = orig
    return c, rows


def test_five_requests_per_customer_in_order_with_their_gets():
    c, rows = run_with({"first": 3, "next1": 2, "next2": 2, "next3": 1, "hot": 0})
    assert [r["kind"] for r in rows[:5]] == ["first", "next1", "next2", "next3", "hot"]
    assert [r["gets"] for r in rows[:5]] == [3, 2, 2, 1, 0] and rows[0]["get_bytes"] == 3000
    colls = [x[1] for x in c.log[:5]]
    assert colls == ["messaggi", "eventi", "ordini", "messaggi", "messaggi"], colls
    # NEXT 3 is an older page: a ts bound taken from what FIRST read
    assert '"$lt"' in c.log[3][2], c.log[3]


def test_summary_verdicts_and_gets_zero():
    _, rows = run_with({"first": 3, "next1": 0, "next2": 0, "next3": 2, "hot": 0})
    s = fn.summarize(rows)["all"]
    assert s["cold_customers"] == 4 and s["errors"] == 0
    assert s["next1"]["against_hot"]["gets_zero"] is True
    assert s["next3"]["against_hot"]["gets_zero"] is False
    assert s["next1"]["against_hot"]["p50_ms"]["verdict"] in ("FASTER", "EQUAL", "SLOWER")
    txt = fn.text(fn.summarize(rows))
    assert "NEXT 3 messaggi older page" in txt and "GETs NOT 0" in txt and "GETs 0" in txt


def test_a_customer_not_cold_at_first_is_counted_and_left_out():
    _, rows = run_with({"first": 3, "next1": 1, "next2": 1, "next3": 1, "hot": 0}, warm={"cli-000000"})
    s = fn.summarize(rows)["all"]
    assert s["customers"] == 4 and s["cold_customers"] == 3 and s["not_cold_at_first"] == 1
    assert s["first"]["n"] == 3


def test_verdict_is_firstnext_s():
    assert fn.verdict(0.89) == "FASTER" and fn.verdict(1.0) == "EQUAL" and fn.verdict(1.1) == "EQUAL" and fn.verdict(1.11) == "SLOWER"


def test_state_files_found_recursively_largest_first_in_main():
    d = tempfile.mkdtemp()
    for db, b in (("small", 1), ("big", 9)):
        os.makedirs(os.path.join(d, "x", db))
        json.dump({"db": db, "clienti": {"n": 5, "nomi_da": "cli-%06d", "riservati": []}, "caricamento": {"byte_bson": b}},
                  open(os.path.join(d, "x", db, "risultati_p1.json"), "w"))
    st = fn.find_states(d)
    assert set(st) == {"small", "big"}


def test_wired_into_the_round():
    ex = open(os.path.join(S9, "machine", "extras.sh")).read()
    # aws-local-round: both read the pantry as loaded, so both check the writing mark first
    assert ex.index("if has first-next && reads_as_loaded_ok first-next; then") < \
        ex.index("if has ballast-rounds && reads_as_loaded_ok ballast-rounds; then")
    assert "with_status first-next out/first-next/first-next-summary.json $PY first_next.py" in ex
    dc = open(os.path.join(S9, "ci", "decide.py")).read()
    assert '"first-next", "ballast-rounds"' in dc and '"first_next_args": "--customers 30 --max-dbs 2"' in dc
    for f, want in (("03-M2-extras-write-peak.json", True), ("06-B-ballast-rounds-100gb.json", True)):
        c = json.load(open(os.path.join(S9, "commands", f)))
        assert ("first-next" in c["programs"]) is want, f
