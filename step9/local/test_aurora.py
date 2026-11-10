"""Step 9 · local test of ci/aurora.py against moto (an offline imitation of the AWS RDS and EC2 APIs: no AWS call, $0).
What it proves: the create / status / delete / leftovers / teardown calls are well formed (moto validates the request
shapes boto3 sends), the names and tags are ours, the deletion leaves nothing (no snapshot), the teardown finds a cluster
of any run, the permission check's probes are refused "after the permission check" (moto has no IAM, so it proves only
the call shapes and the not-found answers, not the policy), and the policy JSON is valid and limited to our names.
What it cannot prove: IAM itself (only `aurora.py permissions` on AWS does), RDS's real timings and engine versions.
Usage: python -m pytest -q step9/local/test_aurora.py"""
import json
import os
import sys

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ci"))
os.environ.update(AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing", AWS_DEFAULT_REGION="eu-west-1")
import aurora  # noqa: E402


def left():
    """What the final check would list. moto keeps a deleted cluster's automated snapshot even when the deletion asks to
    remove automated backups; AWS removes it (DeleteAutomatedBackups, the default for Aurora), and on AWS the final
    check lists it if it stayed. So here the automated snapshots of deleted clusters are left out."""
    alive = {c["DBClusterIdentifier"] for c in boto3.client("rds", region_name="eu-west-1").describe_db_clusters()["DBClusters"]}
    return [x for x in aurora.leftovers() if not ("(automated)" in x and not any(a in x for a in alive))]


@pytest.fixture
def aws():
    with mock_aws():
        ec2 = boto3.client("ec2", region_name="eu-west-1")
        assert ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"], "moto has a default VPC"
        yield


def test_create_ready_delete_leaves_nothing(aws, monkeypatch):
    monkeypatch.setattr(aurora, "engine_version", lambda rds: "16.6")
    n = aurora.create("101", "a" * 48, say=lambda m: None)
    cs, ins, ep = aurora.status("101")
    assert cs in ("available", "creating") and ins in ("available", "creating") and ep
    rds = boto3.client("rds", region_name="eu-west-1")
    c = rds.describe_db_clusters(DBClusterIdentifier=n["cluster"])["DBClusters"][0]
    assert c["Engine"] == "aurora-postgresql" and c["StorageEncrypted"] and c["DatabaseName"] == "app_demo"
    tags = {t["Key"]: t["Value"] for t in rds.list_tags_for_resource(ResourceName=c["DBClusterArn"])["TagList"]}
    assert tags["project"] == "fenvix-coldread" and tags["Name"] == n["cluster"]
    i = rds.describe_db_instances(DBInstanceIdentifier=n["instance"])["DBInstances"][0]
    assert i["DBInstanceClass"] == "db.r7g.large" and not i["PubliclyAccessible"]
    assert "sslmode=require" in aurora.uri(ep, "p@ss/w") and "p%40ss%2Fw" in aurora.uri(ep, "p@ss/w")
    assert len(left()) == 4          # cluster, instance, subnet group, the cluster's automated backup while it lives
    assert aurora.delete("101", say=lambda m: None, wait=lambda: None)
    assert left() == []              # no final snapshot, nothing left


def test_teardown_finds_any_run(aws, monkeypatch):
    monkeypatch.setattr(aurora, "engine_version", lambda rds: "16.6")
    aurora.create("7", "b" * 48, say=lambda m: None)
    aurora.create("8", "c" * 48, say=lambda m: None)
    assert aurora.teardown(say=lambda m: None) == ["7", "8"]
    assert left() == []


def test_delete_is_idempotent(aws):
    assert aurora.delete("404", say=lambda m: None, wait=lambda: None)


def test_permission_probes_are_refused_after_the_check(aws, monkeypatch):
    monkeypatch.setattr(aurora, "engine_version", lambda rds: "16.6")
    lines, missing = aurora.permissions("55")
    print("\n".join(lines))
    # moto has no IAM and does not check that a DB subnet group exists: the CreateDBCluster probe makes a cluster there
    # (AWS refuses it). The safety net must report it and delete it at once; everything else must be OK.
    assert set(missing) <= {"probe", "leftovers"}, lines
    assert any("deleted at once" in x for x in lines)
    assert any("CreateDBInstance" in x and "refused after the permission check" in x for x in lines)
    assert left() == []


def test_policy_is_least_privilege_and_names_fit_it():
    p = aurora.policy()
    text = json.dumps(p)
    assert "<ACCOUNT_ID>" in text and not any(ch.isdigit() and len(x) == 12 for x in text.split(":") for ch in x[:1])
    allow = [s for s in p["Statement"] if s["Effect"] == "Allow"]
    for s in allow:
        acts = s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
        assert not any(a.endswith("*") for a in acts), s["Sid"]
        if any(a.startswith("rds:Create") or a.startswith("rds:Delete") for a in acts):
            assert all(":fenvix-coldread-*" in r for r in s["Resource"]), s["Resource"]
        if s["Resource"] == "*":
            assert all(a.startswith("rds:Describe") or a.startswith("rds:List") for a in acts), acts
    for run in ("42792335572", "7", "perm-42792335572"):
        for kind, name in aurora.names(run).items():
            assert aurora.name_ok(name, kind), name
    assert not aurora.name_ok("app-demo", "cluster") and not aurora.name_ok("fenvix-coldread-x-", "cluster")
