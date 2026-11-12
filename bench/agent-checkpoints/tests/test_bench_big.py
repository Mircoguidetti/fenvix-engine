"""P-bench-big (2026-11-12): the new profile, the extra turns of the mix, thread picking, the probes' arithmetic, the
per-round summaries and the s3delay proxy."""
import os
import random
import socket
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import bigmix  # noqa: E402
import generator  # noqa: E402


def test_standard_plan_unchanged_and_big_is_bigger():
    t, e = generator.plan(42, "standard")
    assert generator.plan_digest(e) == "cc5dfd2ec7d93f21"   # the digest of every earlier standard run
    tb, eb = generator.plan(42, "big")
    assert len(tb) > 1.9 * len(t) and len(eb) > 1.9 * len(e)


def test_extend_thread_keeps_planned_turns_and_is_deterministic():
    th = generator.plan(42, "small")[0][3]
    x1, x2 = generator.extend_thread(42, th, 4), generator.extend_thread(42, th, 4)
    assert x1.tool_rounds[: th.turns] == th.tool_rounds and x1.turns == th.turns + 4
    assert x1.tool_rounds == x2.tool_rounds
    assert generator.turn_messages(42, x1, th.turns + 3) == generator.turn_messages(42, x2, th.turns + 3)
    # extending by more turns later gives the same earlier extra turns (round B continues where round A stopped)
    assert generator.extend_thread(42, th, 6).tool_rounds[: th.turns + 4] == x1.tool_rounds


def test_pick_threads_disjoint_across_rounds_and_same_for_every_target():
    threads = {f"t{i:04d}": None for i in range(500)}
    used = set()
    w1, r1 = bigmix.pick_threads(threads, used, 50, 60, 1000)
    used |= set(w1) | set(r1)
    w2, r2 = bigmix.pick_threads(threads, used, 50, 60, 2000)
    assert not (set(w1) | set(r1)) & (set(w2) | set(r2))
    assert (w1, r1) == bigmix.pick_threads(threads, set(), 50, 60, 1000)


def test_op_keys():
    assert bigmix.op_key({"op": "resume", "gap": "long"}) == "resume[long]"
    assert bigmix.op_key({"op": "resume", "gap": "none"}) == "resume[none]"
    assert bigmix.op_key({"op": "resume", "gap": "read"}) == "resume[read]"
    assert bigmix.op_key({"op": "save_step", "gap": "long"}) == "save_step[old]"
    assert bigmix.op_key({"op": "save_step", "gap": "none"}) == "save_step[none]"
    assert bigmix.op_key({"op": "put", "gap": "none"}) is None
    assert bigmix.op_key({"op": "list_limit10"}) == "list_limit10"


def test_summarize_round_counts_absent_counters_as_zero():
    rows = [{"phase": "mix:A", "op": "resume", "gap": "long", "ms": 10.0, "res": {"srv.s3.get": 4}},
            {"phase": "mix:A", "op": "resume", "gap": "long", "ms": 20.0},
            {"phase": "mix:B", "op": "resume", "gap": "long", "ms": 99.0, "res": {"srv.s3.get": 9}}]
    s = bigmix.summarize_round(rows, "A")["resume[long]"]
    assert s["n"] == 2 and s["res_mean"]["srv.s3.get"] == 2.0 and s["p99"] == 20.0


def test_probe_delta_and_cache_rates():
    assert bigmix.Probe.delta({"a": 1, "b": 5}, {"a": 4, "b": 5, "c": 9}) == {"a": 3}
    r = bigmix.cache_rates("mongo", {"pages requested from the cache": 100, "pages read into cache": 10},
                           {"pages requested from the cache": 300, "pages read into cache": 60})
    assert r["wt_hit_rate"] == 0.75
    r = bigmix.cache_rates("postgres", {"heap_read": 0, "heap_hit": 0}, {"heap_read": 1, "heap_hit": 3})
    assert r["shared_buffers_hit_rate"] == 0.75
    r = bigmix.cache_rates("fenvix", {"cache.hits": 0, "cache.misses": 0, "s3.get": 1}, {"cache.hits": 9, "cache.misses": 1, "s3.get": 5})
    assert r["block_cache_hit_rate"] == 0.9 and r["s3_gets"] == 4


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_s3delay_adds_the_delay_to_each_response_once():
    binary = os.path.join(HERE, "..", "..", "..", "target", "release", "s3delay")
    if not os.path.exists(binary):
        import pytest
        pytest.skip("target/release/s3delay not built")
    up, front = _free_port(), _free_port()
    srv = socket.socket()
    srv.bind(("127.0.0.1", up))
    srv.listen(1)

    def echo():
        c, _ = srv.accept()
        while True:
            d = c.recv(65536)
            if not d:
                break
            c.sendall(b"R" * 200_000)   # a long answer: only its first bytes wait
    threading.Thread(target=echo, daemon=True).start()
    p = subprocess.Popen([binary, f"127.0.0.1:{front}", f"127.0.0.1:{up}", "50"], stderr=subprocess.DEVNULL)
    try:
        time.sleep(0.3)
        c = socket.create_connection(("127.0.0.1", front))
        for _ in range(3):
            t0 = time.perf_counter()
            c.sendall(b"GET / HTTP/1.1\r\n\r\n")
            got = 0
            while got < 200_000:
                got += len(c.recv(65536))
            dt = time.perf_counter() - t0
            assert 0.045 < dt < 0.090, dt   # one delay per request, not one per chunk
        c.close()
    finally:
        p.kill()


def test_big_settings_of_the_lines():
    import importlib
    import lines
    importlib.reload(lines)
    assert lines.LIMITS == ["--cpus", "1", "--memory", "3g"] and lines.MONGO_CACHE() == []
    lines.set_big("1g")
    try:
        assert lines.LIMITS == ["--cpus", "1", "--memory", "1g"]
        assert lines.PG_MEM_FLAGS == []          # Postgres shared_buffers at its default
        assert lines.MONGO_CACHE() == ["--wiredTigerCacheSizeGB", "0.25"]
    finally:
        importlib.reload(lines)


def test_stable_needs_every_poll():
    import lines
    seq = iter([True, True, False, True])
    assert lines.stable(lambda: next(seq), 4, every=0) is False
    assert lines.stable(lambda: True, 3, every=0) is True
