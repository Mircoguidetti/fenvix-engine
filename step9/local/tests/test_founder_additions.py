"""Founder's decisions for the AWS round (2026-11-13), added by the review of the AWS harness:
  1. Atlas at 100 GB: phase "atlas" (machine/atlas_client.sh), jobs 12-A1, 13-A2, 14-A3; first-next without counters;
  2. where we are stronger: programs/data_ops.py (ours: copy = fork, restore to a past time, through the admin
     interface), the "handover" program (takeovers.py --how stop, the product's 30 s lease) and programs/atlas_ops.py
     (Atlas: Test Failover, restore to another cluster, point-in-time restore, through the Admin API, digest auth);
  3. idle cost: programs/idle_cost.py (S3 requests per second and dollars per month of an open database, no traffic).
Nothing here reaches Atlas or AWS: the Admin API and our admin interface are fakes on 127.0.0.1."""
import datetime as dt
import http.server
import json
import os
import subprocess
import sys
import threading
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
CMDS = os.path.join(S9, "commands")
sys.path.insert(0, os.path.join(S9, "programs"))
sys.path.insert(0, os.path.join(S9, "ci"))
import atlas_ops  # noqa: E402
import data_ops  # noqa: E402
import idle_cost  # noqa: E402


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("S9_MODE", "local")
    monkeypatch.setenv("RUN_ID", "1000000001")


def _resolve(name, **over):
    import decide
    raw = json.load(open(os.path.join(CMDS, name)))
    raw.update(over)
    return decide.resolve(raw)


def _refused(raw, capsys):
    import decide
    with pytest.raises(SystemExit):
        decide.resolve(raw)
    return capsys.readouterr().out


class Clock:
    def __init__(self):
        self.t = 1000.0

    def sleep(self, s):
        self.t += s

    def now(self):
        return self.t


# ---------- a fake MongoDB: messaggi per customer, with a wall clock for restores ----------
class FakeMongo:
    def __init__(self, clock, data=None, primary="a:1"):
        self.clock, self.dbs, self.primary, self.requests = clock, data if data is not None else {}, primary, {}
        self.admin = self
        self.counts = {k: 0 for k in idle_cost.KINDS}

    def list_database_names(self):
        return list(self.dbs) + ["admin"]

    def command(self, name, *a, **k):
        if name == "tenantStats":
            if self.counts is None:
                raise RuntimeError("no such command")
            self.counts["get"] += 3          # 3 GETs and 1 LIST per read of the counters: the 'idle' traffic
            self.counts["list"] += 1
            return {"requests": dict(self.counts)}
        if name == "hello":
            return {"primary": self.primary}
        if name == "dbStats":
            return {"dataSize": 123}
        raise RuntimeError(name)

    def __getitem__(self, db):
        m = self

        class Coll:
            def __init__(self, name):
                self.name = name

            def with_options(self, **kw):
                return self

            def find_one(self, q, sort=None, **kw):
                rows = [r for r in m.dbs.get(db, {}).get(q["cliente"], [])]
                return max(rows, key=lambda r: r["t"]) if rows else None

            def insert_one(self, d):
                m.clock.t += 0.01
                cli = d.get("cliente", "_")
                m.dbs.setdefault(db, {}).setdefault(cli, []).append({"_id": f"{cli}-{len(m.dbs[db][cli])}-{m.clock.t}", "t": m.clock.t})

        class DB:
            def __getitem__(self, c):
                return Coll(c)

            def command(self, name):
                return m.command(name)

            def drop_collection(self, n):
                m.dbs.get(db, {}).pop("_", None)
        return DB()


STATE = {"db": "fenvix_p1", "clienti": {"nomi_da": "cli-%06d", "n": 50, "riservati": []}, "caricamento": {"byte_bson": 1e10}}


def _states(tmp_path):
    os.makedirs(tmp_path / "s" / "fenvix_p1")
    json.dump(STATE, open(tmp_path / "s" / "fenvix_p1" / "risultati_p1.json", "w"))
    return str(tmp_path / "s")


def _seed(m):
    m.dbs["fenvix_p1"] = {f"cli-{i:06d}": [{"_id": f"cli-{i:06d}-0", "t": 1.0}] for i in range(50)}


# ---------- a fake HTTP server (ours or Atlas's) ----------
class Fake(http.server.BaseHTTPRequestHandler):
    routes = None
    digest = False
    calls = None

    def log_message(self, *a):
        pass

    def _go(self, method):
        if self.digest and not (self.headers.get("Authorization") or "").startswith("Digest "):
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Digest realm="MMS Public API", nonce="abc", qop="auth", algorithm=MD5')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"null") if n else None
        self.calls.append((method, self.path, body, self.headers.get("Authorization", "")[:7]))
        st, out = self.routes(method, self.path, body)
        b = json.dumps(out).encode()
        self.send_response(st)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        self._go("GET")

    def do_POST(self):
        self._go("POST")

    def do_PUT(self):
        self._go("PUT")

    def do_DELETE(self):
        self._go("DELETE")


def serve(routes, digest=False):
    h = type("H", (Fake,), {"routes": staticmethod(routes), "digest": digest, "calls": []})
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), h)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, h.calls, f"http://127.0.0.1:{srv.server_address[1]}"


# ---------- 3. idle cost ----------
def test_idle_cost_counts_requests_per_second_and_dollars_per_month(tmp_path, monkeypatch):
    clock = Clock()
    m = FakeMongo(clock)
    _seed(m)
    fake = types.ModuleType("pymongo")
    fake.MongoClient = lambda *a, **k: m
    monkeypatch.setitem(sys.modules, "pymongo", fake)
    rc = idle_cost.main([str(tmp_path / "o"), "--uri", "x", "--states", _states(tmp_path), "--settle-s", "60", "--idle-s", "600"],
                        sleep=clock.sleep, now=clock.now)
    s = json.load(open(tmp_path / "o" / "idle-cost-summary.json"))
    assert rc == 0 and s["ok"] and s["ours"] and s["db"] == "fenvix_p1"
    # between the two reads of the measure: one counter read's own 3 GETs and 1 LIST over 600 s
    assert s["requests_per_s"]["get"] == round(3 / 600, 6) and s["requests_per_s"]["list"] == round(1 / 600, 6)
    want = (3 / 600 * 0.0004 + 1 / 600 * 0.005) / 1000 * 30 * 86400
    assert abs(s["dollars_per_month"] - round(want, 4)) < 1e-4
    # never flat in this fake (every read moves it): the settle waits its whole cap and says so
    assert s["settled"] is False and s["settle_s"] >= 60


def test_idle_cost_on_a_server_that_is_not_ours_measures_nothing(tmp_path, monkeypatch):
    m = FakeMongo(Clock())
    m.counts = None
    fake = types.ModuleType("pymongo")
    fake.MongoClient = lambda *a, **k: m
    monkeypatch.setitem(sys.modules, "pymongo", fake)
    assert idle_cost.main([str(tmp_path / "o"), "--uri", "x", "--db", "d"]) == 0
    s = json.load(open(tmp_path / "o" / "idle-cost-summary.json"))
    assert s["ours"] is False and "price per hour" in s["text"]


def test_idle_cost_cost_formula_matches_the_s3_prices():
    assert abs(idle_cost.cost({"get": 375.0}) - 375 * 2592000 * 0.0004 / 1000) < 1e-6   # fix-box-first-trip's 375 GET/s
    assert idle_cost.cost({"delete": 10.0}) == 0.0


# ---------- 2. ours: copy and restore to a past time ----------
def _our_admin(m, clock):
    def routes(method, path, body):
        parts = path.strip("/").split("/")
        if method == "POST" and parts[-1] == "fork":
            m.dbs[body["target"]] = {k: list(v) for k, v in m.dbs[parts[2]].items()}
            return 201, {"database": body["target"]}
        if method == "PUT" and parts[-1] == "retention":
            return 200, body
        if method == "GET" and parts[-1] == "restore-points":
            return 200, {"points": [{"at": 1}]}
        if method == "POST" and parts[-1] == "restore":
            to = body["to"] / 1000.0
            for cli, rows in m.dbs[parts[2]].items():
                rows[:] = [r for r in rows if r["t"] <= to]
            return 200, {"restored_to": body["to"]}
        if method == "DELETE":
            m.dbs.pop(parts[2], None)
            return 200, {}
        if method == "GET" and len(parts) == 3:
            return 200, {"name": parts[2], "operation": None}
        return 404, {}
    return routes


def test_data_ops_copy_and_restore_are_timed_and_checked(tmp_path, monkeypatch):
    clock = Clock()
    m = FakeMongo(clock)
    _seed(m)
    srv, calls, url = serve(_our_admin(m, clock))
    try:
        admin = data_ops.Admin(url, "t" * 20, 30)
        cust = [f"cli-{i:06d}" for i in range(5)]
        c = data_ops.op_copy(m, admin, "fenvix_p1", cust, 60, sleep=clock.sleep, now=clock.now)
        assert c["ok"] and c["answers_like_source"] and c["erase_copy"][0] == 200 and "fenvix_p1_s9copy" not in m.dbs
        fake_pm = types.ModuleType("pymongo.write_concern")
        fake_pm.WriteConcern = lambda **k: None
        monkeypatch.setitem(sys.modules, "pymongo.write_concern", fake_pm)
        r = data_ops.op_restore(m, admin, "fenvix_p1", cust, 40, 60, sleep=clock.sleep, now=clock.now, wall=clock.now)
        assert r["ok"] and r["restored_like_target"] and r["changed_since_target"] and r["restore_point_seen"]
        assert ("PUT", "/v1/databases/fenvix_p1/retention", {"window_secs": 7200, "interval_secs": 30}, "Bearer ") in calls
        # a restore that did nothing is caught
        def no_restore(method, path, body):
            return (200, {}) if path.endswith("/restore") else _our_admin(m, clock)(method, path, body)
        srv2, _, url2 = serve(no_restore)
        r2 = data_ops.op_restore(m, data_ops.Admin(url2, "t" * 20, 30), "fenvix_p1", cust, 40, 5, sleep=clock.sleep,
                                 now=clock.now, wall=clock.now)
        assert not r2["ok"] and not r2["restored_like_target"]
        srv2.shutdown()
    finally:
        srv.shutdown()


# ---------- 2. Atlas: failover, copy, restore to a past time ----------
def test_atlas_ops_against_a_fake_admin_api_with_digest_auth(tmp_path, monkeypatch):
    clock = Clock()
    src = FakeMongo(clock, primary="a:27017")
    _seed(src)
    tgt = FakeMongo(clock, data={})
    jobs = {}

    def routes(method, path, body):
        p = path.split("/api/atlas/v2")[-1]
        if method == "GET" and p == "/groups/G/clusters/C":
            return 200, {"stateName": "IDLE", "pitEnabled": True, "backupEnabled": True}
        if method == "POST" and p.endswith("/restartPrimaries"):
            src.primary = "b:27017"
            return 200, {}
        if method == "POST" and p.endswith("/backup/snapshots"):
            jobs["snap"] = {k: list(v) for k, v in src.dbs["fenvix_p1"].items()}
            return 202, {"id": "s1"}
        if method == "GET" and p.endswith("/backup/snapshots/s1"):
            return 200, {"status": "completed"}
        if method == "POST" and p.endswith("/backup/restoreJobs"):
            if body["deliveryType"] == "automated":
                tgt.dbs["fenvix_p1"] = jobs["snap"]
            else:
                for rows in src.dbs["fenvix_p1"].values():
                    rows[:] = [r for r in rows if r["t"] <= body["pointInTimeUTCSeconds"]]
            return 200, {"id": "j1"}
        if method == "GET" and p.endswith("/backup/restoreJobs/j1"):
            return 200, {"finishedAt": "2026-11-18T10:00:00Z"}
        return 404, {}
    srv, calls, url = serve(routes, digest=True)
    wc = types.ModuleType("pymongo.write_concern")
    wc.WriteConcern = lambda **k: None
    monkeypatch.setitem(sys.modules, "pymongo.write_concern", wc)
    monkeypatch.setattr(atlas_ops.time, "time", clock.now)
    try:
        key = tmp_path / "k"
        key.write_text("pub:priv")
        clients = {"src": src, "tgt": tgt}
        rc = atlas_ops.main([str(tmp_path / "o"), "--uri", "src", "--target-uri", "tgt", "--group", "G", "--cluster", "C",
                             "--key-file", str(key), "--states", _states(tmp_path), "--target-cluster", "T",
                             "--api", url + "/api/atlas/v2", "--cap-s", "600"],
                            sleep=clock.sleep, now=clock.now, client_factory=lambda u: clients[u])
        s = json.load(open(tmp_path / "o" / "atlas-ops-summary.json"))
        assert rc == 0 and s["ok"], s
        f = s["ops"]["failover"]
        assert f["primary_before"] == "a:27017" and f["primary_after"] == "b:27017" and f["seconds_to_new_primary"] is not None
        assert s["ops"]["copy"]["answers_like_source"] and s["ops"]["restore"]["restored_like_target"]
        # digest auth answered the challenge; the restore to a past time names the same cluster, at a whole second
        assert all(a == "Digest " for _, _, _, a in calls)
        pit = [b for m_, p, b, _ in calls if p.endswith("/restoreJobs") and b["deliveryType"] == "pointInTime"][0]
        assert pit["targetClusterName"] == "C" and isinstance(pit["pointInTimeUTCSeconds"], int)
    finally:
        srv.shutdown()


def test_atlas_ops_says_why_when_pitr_is_off(tmp_path, monkeypatch):
    clock = Clock()
    src = FakeMongo(clock)
    _seed(src)
    srv, calls, url = serve(lambda m, p, b: (200, {"stateName": "IDLE", "pitEnabled": False}), digest=True)
    try:
        key = tmp_path / "k"
        key.write_text("pub:priv")
        rc = atlas_ops.main([str(tmp_path / "o"), "--uri", "s", "--group", "G", "--cluster", "C", "--key-file", str(key),
                             "--states", _states(tmp_path), "--ops", "restore", "--api", url],
                            sleep=clock.sleep, now=clock.now, client_factory=lambda u: src)
        s = json.load(open(tmp_path / "o" / "atlas-ops-summary.json"))
        assert rc == 1 and "pitEnabled" in s["ops"]["restore"]["why"]
    finally:
        srv.shutdown()


# ---------- 1. the Atlas phase, and the wiring ----------
def test_the_three_atlas_jobs_resolve_alone_on_one_client_within_the_ceiling():
    a1, a2, a3 = (_resolve(f) for f in ("12-A1-atlas-ops-atlas-size.json", "13-A2-atlas-load-100gb.json",
                                         "14-A3-atlas-read-rounds-100gb.json"))
    for c in (a1, a2, a3):
        assert c["phases"] == ["atlas"] and c["vcpu_peak"] == 4 and c["job_hours_total"] <= 0.9 * 330 / 60
        assert c["generator"] is None and c["filler"] is None and c["pantry_mode"] == "existing"
    assert a1["atlas_load_states"] == ["vecchio"] and "atlas-ops" in a1["programs"] and a1["atlas_ballast_line"] == []
    # A2 loads the SAME line as our 100 GB generator (same name, size and seed): the same customers
    g = _resolve("05-G-generate-100gb.json")
    assert a2["atlas_ballast_line"][:5] == g["ballast"]["lines"][0][:5] == ["app_data_h01", "--gb", "93.2", "--seme", "2001"]
    assert a3["ballast_db"] == "app_data_h01" and a3["programs"] == ["first-next", "ballast-rounds"]


def test_the_atlas_phase_refuses_what_it_cannot_do(capsys):
    base = json.load(open(os.path.join(CMDS, "12-A1-atlas-ops-atlas-size.json")))
    assert "alone" in _refused({**base, "phases": ["atlas", "measure"]}, capsys)
    assert "programs" in _refused({**base, "programs": ["m30"]}, capsys)
    assert "pantry" in _refused({**base, "pantry": "new"}, capsys)
    assert "atlas-load" in _refused({**base, "atlas_load_states": []}, capsys)
    assert "atlas_ballast" in _refused({**base, "atlas_ballast": True}, capsys)


def test_data_ops_turns_on_the_admin_interface_and_the_token_is_never_in_a_command(capsys):
    m3 = _resolve("04-M3-agent-bench.json")
    assert m3["programs"] == ["idle-cost", "data-ops", "handover", "agent-bench"]
    assert "--admin-listen 0.0.0.0:8080" in m3["server_options"] and "data-ops" in __import__("decide").WRITING_PROGRAMS
    assert "--how stop" in m3["handover_args"] and "--lease-ttl 30" in m3["handover_args"]
    raw = json.load(open(os.path.join(CMDS, "04-M3-agent-bench.json")))
    assert "set by the machine scripts" in _refused({**raw, "server_options": raw["server_options"] + " --admin-token x" * 1}, capsys)
    cook = open(os.path.join(S9, "machine", "cook.sh")).read()
    assert 'admin_token > admin-token && chmod 600 admin-token && EXTRA+=(--admin-token-file "$PWD/admin-token")' in cook
    common = open(os.path.join(S9, "machine", "common.sh")).read()
    assert "admin_token() { printf 's9-admin:%s' \"$CM_PW\" | sha256sum | cut -c1-40; }" in common


def test_the_idle_cost_runs_first_on_each_size_and_takeovers_can_hand_over_gracefully():
    for f, db in (("04-M3-agent-bench.json", None), ("06-BW-read-rounds-ops-raw-100gb.json", "app_data_h01"),
                  ("09-B1-read-rounds-1tb.json", "app_data_b01")):
        c = _resolve(f)
        assert c["programs"][0] == "idle-cost" and c["ballast_db"] == db and c["job_hours"]["idle_cost"] == 0.3
    sh = open(os.path.join(S9, "machine", "extras.sh")).read()
    assert sh.index("if has idle-cost; then") < sh.index("if has first-next && reads_as_loaded_ok first-next; then")
    assert sh.index("if has reads-after-writes; then") < sh.index("if has data-ops; then") < sh.index("if has takeovers; then")
    tk = open(os.path.join(S9, "programs", "takeovers.py")).read()
    assert 'ack = command(victim, a.how)' in tk and 'choices=("kill9", "stop")' in tk


def test_first_next_without_counters_takes_every_first_request_as_cold():
    import first_next as fn
    rows = [{"db": "d", "cliente": f"c{i}", "kind": k, "ms": 1.0, "error": None, "gets": 0, "get_bytes": 0,
             "maintenance_gets": 0, "counted": False} for i in range(3) for k in ("first", "next1", "next2", "next3", "hot")]
    s = fn.summarize(rows)
    assert s["all"]["cold_customers"] == 3 and s["all"]["next1"]["n"] == 3
    for r in rows:
        r["counted"] = True
    assert fn.summarize(rows)["all"]["cold_customers"] == 0          # ours, with counters: 0 GETs is not cold


def test_the_atlas_secrets_are_masked_and_never_published(tmp_path):
    wf = open(os.path.join(S9, "..", ".github", "workflows", "step9-kitchen.yml")).read()
    for sname in ("STEP9_ATLAS_URI", "STEP9_ATLAS_API_KEY", "STEP9_ATLAS_GROUP_ID", "STEP9_ATLAS_CLUSTER"):
        assert f"secrets.{sname}" in wf
    assert "::add-mask::{x}" in wf
    out = tmp_path / "out"
    out.mkdir()
    (out / "a.txt").write_text("the Atlas password is s3cretPassw0rd here")
    env = {**os.environ, "CM_PW": "x" * 20, "ATLAS_URI": "mongodb+srv://u:s3cretPassw0rd@c.mongodb.net/"}
    r = subprocess.run([sys.executable, os.path.join(S9, "..", "kitchen", "ci", "check_secrets.py"), str(out)], env=env,
                       capture_output=True, text=True)
    assert r.returncode == 1
    run = open(os.path.join(S9, "ci", "run.py")).read()
    assert 'put("secret/atlas.json", sec)' in run
    ac = open(os.path.join(S9, "machine", "atlas_client.sh")).read()
    assert "delete_object(Bucket=sys.argv[1], Key='secret/atlas.json')" in ac and "umask 077" in ac


def test_the_round_estimate_has_the_atlas_bill_apart(capsys):
    import estimate
    estimate.round_plan()
    out = capsys.readouterr().out
    assert "Atlas bill (the founder's Atlas account, not AWS)" in out and estimate.round_plan.atlas_bill > 5
