#!/usr/bin/env python3
"""Offline tests of the orchestrator's spend and safety logic (no AWS: clock injected, AWS clients stubbed).
run: python3 tools/walbench/tests/test_orchestrate.py"""
import os
import sys
import tempfile

os.environ.setdefault("WALBENCH_OUT", tempfile.mkdtemp())
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import orchestrate as o  # noqa: E402

F = {k: 0.0 for k in ("s3_get", "s3_gb_month", "xz_put", "xz_get", "xz_upload_gb", "xz_retrieval_gb", "xz_gb_month", "ddb_wru", "ddb_rru",
                      "ddb_gb_month", "inter_az_gb", "gp3_gb_month")}
F.update(s3_put=0.005 / 1000, ec2_hour=0.0912, ipv4_hour=0.005)
CLOCK = [1_000_000.0]
o.now = lambda: CLOCK[0]


def reset():
    o.INST_T.clear()
    o.ST.clear()
    o.Live.last.clear()
    o.VOL_T0[0] = None


def test_terminated_nodes_add_no_staleness_margin():
    reset()
    t0 = CLOCK[0]
    o.ST["instances"] = {"client": "i-c", "node0": "i-0", "node1": "i-1", "node2": "i-2"}
    for i in o.ST["instances"].values():
        o.INST_T[i] = [t0, None]
    for r in ("client", "node0", "node1", "node2"):
        o.Live.last[r] = (t0, {"s3_put": 10})
    # quorum done at t0 + 300: nodes terminated, their progress stops; the client keeps uploading
    CLOCK[0] = t0 + 300
    for i in ("i-0", "i-1", "i-2"):
        o.INST_T[i][1] = CLOCK[0]
    margins = []
    for minute in range(1, 61):  # one hour later
        CLOCK[0] = t0 + 300 + minute * 60
        o.Live.last["client"] = (CLOCK[0] - 20, {"s3_put": 10})
        r = o.Live.compute(F, fetch=False)
        margins.append(r["margin_usd"] - 0.10 * r["requests_usd"])
    # only the client contributes: 30 s (floor) x 2000 PUT/s x price, constant; terminated nodes add nothing
    want = 30 * 2000 * F["s3_put"]
    assert all(abs(m - want) < 1e-6 for m in margins), (margins[0], margins[-1], want)
    total = o.Live.compute(F, fetch=False)["total_usd"]
    assert total < 4.0, total
    print(f"ok: staleness margin stays {want:.3f} USD for an hour after the nodes end (total {total:.3f} USD)")


def test_stale_client_margin_grows():
    reset()
    t0 = CLOCK[0]
    o.ST["instances"] = {"client": "i-c"}
    o.INST_T["i-c"] = [t0, None]
    o.Live.last["client"] = (t0, {})
    CLOCK[0] = t0 + 600
    m = o.Live.compute(F, fetch=False)["margin_usd"]  # (no requests counted, so no +10% part)
    assert abs(m - 600 * 2000 * F["s3_put"]) < 1e-6, m
    o.INST_T["i-c"][1] = CLOCK[0]
    assert o.Live.compute(F, fetch=False)["margin_usd"] == 0.0
    print("ok: a silent running client is charged its worst rate; once terminated it is not")


class Stub:
    def __init__(self, **fns):
        self.fns = fns

    def __getattr__(self, k):
        return self.fns[k]


def with_clients(mapping):
    o.c = lambda svc, region=None: mapping[svc]


def test_quota_rule():
    def ec2(used_vcpus):
        class P:
            def paginate(self, **kw):
                yield {"Reservations": [{"Instances": [{"CpuOptions": {"CoreCount": used_vcpus, "ThreadsPerCore": 1}}]}]}
        return Stub(get_paginator=lambda name: P())

    sq = lambda v: Stub(get_service_quota=lambda **kw: {"Quota": {"Value": v}})  # noqa: E731
    with_clients({"service-quotas": sq(32.0), "ec2": ec2(0)})
    assert o.instance_budget()["max_instances"] == 4
    with_clients({"service-quotas": sq(16.0), "ec2": ec2(9)})  # free 7
    assert o.instance_budget()["max_instances"] == 3
    with_clients({"service-quotas": sq(8.0), "ec2": ec2(0)})  # free exactly 8
    assert o.instance_budget()["max_instances"] == 4

    def denied(**kw):
        from botocore.exceptions import ClientError
        raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "GetServiceQuota")
    with_clients({"service-quotas": Stub(get_service_quota=denied), "ec2": ec2(0)})
    r = o.instance_budget()
    assert r["max_instances"] == 3 and r["quota_error"] == "AccessDeniedException", r
    with_clients({"service-quotas": sq(10.0), "ec2": ec2(6)})  # free 4: not even 3 fit
    try:
        o.instance_budget()
        raise AssertionError("expected SystemExit")
    except SystemExit:
        pass
    print("ok: quota rule (4 only if free >= 8; 3 otherwise or on AccessDenied; refuse below 6 free)")


def test_terminate_never_raises():
    reset()
    o.INST_T["i-x"] = [CLOCK[0], None]
    calls = []

    def boom(**kw):
        calls.append(kw)
        raise RuntimeError("network down")
    with_clients({"ec2": Stub(terminate_instances=boom)})
    o.time.sleep = lambda s: None
    assert o.terminate_all() is False and len(calls) == 3
    assert o.INST_T["i-x"][1] is None  # still counted as running in the spend (it may be)
    with_clients({"ec2": Stub(terminate_instances=lambda **kw: calls.append(kw))})
    assert o.terminate_all() is True and o.INST_T["i-x"][1] == CLOCK[0]
    print("ok: terminate retries, never raises, marks the end time only on success")


if __name__ == "__main__":
    test_terminated_nodes_add_no_staleness_margin()
    test_stale_client_margin_grows()
    test_quota_rule()
    test_terminate_never_raises()
    print("all orchestrator tests passed")


def test_empty_bucket_bounded():
    import infra
    lists = [0]

    class S3:
        def list_objects_v2(self, **kw):
            lists[0] += 1
            return {"Contents": [{"Key": "k1"}, {"Key": "k2"}], "IsTruncated": False}

        def delete_objects(self, **kw):
            return {"Errors": [{"Key": "k1", "Code": "AccessDenied"}, {"Key": "k2", "Code": "AccessDenied"}]}

        def list_multipart_uploads(self, **kw):
            return {}
    try:
        infra._empty_bucket(S3(), "b", False)
        raise AssertionError("expected an error")
    except RuntimeError as e:
        assert "not emptying" in str(e) and lists[0] == 5, (str(e), lists[0])
    state = {"left": ["a", "b"]}

    class S3ok(S3):
        def list_objects_v2(self, **kw):
            return {"Contents": [{"Key": k} for k in state["left"]], "IsTruncated": False}

        def delete_objects(self, **kw):
            state["left"] = []
            return {}
    assert infra._empty_bucket(S3ok(), "b", False) == 2
    print("ok: bucket emptying checks Errors and stops after 5 passes without progress")


if __name__ == "__main__":
    test_empty_bucket_bounded()
