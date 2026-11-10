"""Step 9 · Aurora PostgreSQL in the agent benchmark (founder 2026-11-09: Aurora is the production Postgres on AWS).

One Aurora PostgreSQL cluster per round, only when the command says "aurora": true and the agent benchmark runs:
  Aurora Standard storage (not I/O-Optimized), one writer db.r7g.large (2 vCPU, 16 GiB: the smallest non-burstable
  class; twice our machines' memory, which favours Aurora), Single-AZ in eu-west-1a (the cook's and the client's zone),
  in the default VPC, in the default VPC's security group (like every machine of the round: its members reach each
  other, nothing from outside; no security-group permission), a DB subnet group of the default subnets, encrypted with
  the AWS managed key (the default), 1 day of automated backups (Aurora's minimum), no Performance Insights, no
  enhanced monitoring, tag project=fenvix-coldread, names fenvix-coldread-s9-<run>; deleted with no final snapshot and
  without its automated backups. Not in the 8-vCPU EC2 quota (RDS).
The agent client runs on it the same agent benchmark as on the Postgres machine (programs/agent_bench.py, target
"aurora", the LangGraph Postgres saver), with the round's password; then run.py deletes it at once.
Prices DECLARED (AWS Price List AmazonRDS eu-west-1, publication 2026-10-29T07:06:35Z): db.r7g.large Aurora PostgreSQL
Single-AZ $0.304 per hour (I/O-Optimized $0.395), storage $0.11 per GB-month (I/O-Optimized $0.248), I/O $0.22 per
million requests.

Library for run.py, teardown.py and final_check.py; and the $0 permission check:
  aurora.py permissions RUN_ID    -> OK / MANCA lines, exit 1 if anything is missing (no cluster is ever created)
  aurora.py policy                -> the IAM policy JSON the founder adds (least privilege, limited by name to
                                     fenvix-coldread-*, as the founder chose on 2026-11-10; the account id a placeholder)"""
import json
import os
import sys
import time

REGION = "eu-west-1"
TAG = "fenvix-coldread"
PREFIX = "fenvix-coldread-s9-"
ENGINE = "aurora-postgresql"
MAJOR = "16"
CLASS = "db.r7g.large"
PRICE_HOUR = {"db.r7g.large": 0.304}          # DECLARED, see the docstring
STORAGE_GB_MONTH = 0.11                      # DECLARED
IO_PER_MILLION = 0.22                        # DECLARED
# I/O allowance for the SPESA line (ESTIMATED upper bound: the standard profile writes about 0.6 M I/Os, the runbook's
# count, x 3 for reads and WAL = 1.8 M x $0.22/M = $0.40) and storage (2 GB x $0.11 / 730 h per hour: negligible)
IO_ALLOWANCE = 0.40


def names(run):
    base = f"{PREFIX}{run}"
    return {"cluster": f"{base}-aurora", "instance": f"{base}-aurora-1", "subnets": f"{base}-aurora-subnets"}


def tags(name):
    return [{"Key": "project", "Value": TAG}, {"Key": "Name", "Value": name}]


def clients():
    import boto3
    return boto3.client("rds", region_name=REGION), boto3.client("ec2", region_name=REGION)


def default_network(ec2):
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    subnets = [s["SubnetId"] for s in ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc]},
                                                                    {"Name": "default-for-az", "Values": ["true"]}])["Subnets"]]
    sg = ec2.describe_security_groups(Filters=[{"Name": "vpc-id", "Values": [vpc]}, {"Name": "group-name", "Values": ["default"]}])["SecurityGroups"][0]["GroupId"]
    return vpc, subnets, sg


def engine_version(rds):
    """The newest Aurora PostgreSQL 16.x AWS offers in the region (the runbook: latest 16 minor)."""
    vs = [v["EngineVersion"] for v in rds.describe_db_engine_versions(Engine=ENGINE, Filters=[{"Name": "status", "Values": ["available"]}])["DBEngineVersions"]
          if v["EngineVersion"].split(".")[0] == MAJOR]
    if not vs:
        raise RuntimeError(f"no Aurora PostgreSQL {MAJOR}.x available in {REGION}")
    return max(vs, key=lambda v: [int(x) if x.isdigit() else 0 for x in v.split(".")])


def create(run, password, say=print):
    """Start the cluster and its writer (returns at once; `wait_ready` waits). Every resource tagged and named."""
    rds, ec2 = clients()
    n = names(run)
    _vpc, subnets, sg = default_network(ec2)
    version = engine_version(rds)
    rds.create_db_subnet_group(DBSubnetGroupName=n["subnets"], DBSubnetGroupDescription="step9 agent benchmark (Aurora)",
                               SubnetIds=subnets, Tags=tags(n["subnets"]))
    # no StorageType: AWS's default for an Aurora cluster is Aurora Standard (DECLARED, RDS API reference, StorageType)
    rds.create_db_cluster(DBClusterIdentifier=n["cluster"], Engine=ENGINE, EngineVersion=version,
                          MasterUsername="app_user", MasterUserPassword=password, DatabaseName="app_demo",
                          DBSubnetGroupName=n["subnets"], VpcSecurityGroupIds=[sg], StorageEncrypted=True,
                          BackupRetentionPeriod=1, DeletionProtection=False, CopyTagsToSnapshot=False,
                          EnableIAMDatabaseAuthentication=False, Port=5432, Tags=tags(n["cluster"]))
    rds.create_db_instance(DBInstanceIdentifier=n["instance"], DBClusterIdentifier=n["cluster"], Engine=ENGINE,
                           DBInstanceClass=CLASS, AvailabilityZone=f"{REGION}a", PubliclyAccessible=False,
                           EnablePerformanceInsights=False, MonitoringInterval=0, AutoMinorVersionUpgrade=False,
                           Tags=tags(n["instance"]))
    say(f"aurora: creating {n['cluster']} (Aurora PostgreSQL {version}, Standard storage) with writer {n['instance']} "
        f"{CLASS} in {REGION}a, default VPC security group; DECLARED ${PRICE_HOUR[CLASS]}/h")
    return n


def status(run):
    """(cluster status, instance status, writer endpoint) or Nones when absent."""
    rds, _ = clients()
    n = names(run)
    cs = ins = ep = None
    try:
        c = rds.describe_db_clusters(DBClusterIdentifier=n["cluster"])["DBClusters"][0]
        cs, ep = c["Status"], c.get("Endpoint")
    except rds.exceptions.DBClusterNotFoundFault:
        pass
    try:
        ins = rds.describe_db_instances(DBInstanceIdentifier=n["instance"])["DBInstances"][0]["DBInstanceStatus"]
    except rds.exceptions.DBInstanceNotFoundFault:
        pass
    return cs, ins, ep


def uri(endpoint, password):
    import urllib.parse
    return f"postgresql://app_user:{urllib.parse.quote(password, safe='')}@{endpoint}:5432/app_demo?sslmode=require"


def delete(run, say=print, wait=None):
    """Delete the writer, the cluster (no final snapshot, no automated backups kept) and the subnet group. `wait` is
    called between polls (run.py's minute: SPESA lines go on); without it, sleep 20 s. Idempotent."""
    rds, _ = clients()
    n = names(run)
    wait = wait or (lambda: time.sleep(20))
    try:
        rds.delete_db_instance(DBInstanceIdentifier=n["instance"])   # an Aurora writer has no snapshot or backup of its own
        say(f"aurora: deleting {n['instance']}")
    except rds.exceptions.DBInstanceNotFoundFault:
        pass
    except Exception as e:   # already deleting
        say(f"aurora: delete {n['instance']}: {type(e).__name__}: {str(e)[:160]}")
    for _ in range(90):
        if status(run)[1] is None:
            break
        wait()
    try:
        # no final snapshot; automated backups go with the cluster (AWS's default, DeleteAutomatedBackups, also said here)
        rds.delete_db_cluster(DBClusterIdentifier=n["cluster"], SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
        say(f"aurora: deleting {n['cluster']} (no final snapshot)")
    except rds.exceptions.DBClusterNotFoundFault:
        pass
    except Exception as e:
        say(f"aurora: delete {n['cluster']}: {type(e).__name__}: {str(e)[:160]}")
    for _ in range(90):
        if status(run)[0] is None:
            break
        wait()
    try:
        rds.delete_db_subnet_group(DBSubnetGroupName=n["subnets"])
    except rds.exceptions.DBSubnetGroupNotFoundFault:
        pass
    except Exception as e:
        say(f"aurora: delete {n['subnets']}: {type(e).__name__}: {str(e)[:160]}")
    cs, ins, _ = status(run)
    return cs is None and ins is None


def leftovers():
    """Every Aurora/RDS resource of ours still in the account: clusters, instances, subnet groups, snapshots (manual
    and automated) named fenvix-coldread-s9-*. The final check calls it (RIMASTO or ALLARME)."""
    rds, _ = clients()
    left = []
    for c in rds.describe_db_clusters().get("DBClusters", []):
        if c["DBClusterIdentifier"].startswith(PREFIX):
            left.append(f"Aurora cluster {c['DBClusterIdentifier']} ({c['Status']})")
    for i in rds.describe_db_instances().get("DBInstances", []):
        if i["DBInstanceIdentifier"].startswith(PREFIX):
            left.append(f"RDS instance {i['DBInstanceIdentifier']} ({i['DBInstanceStatus']})")
    for g in rds.describe_db_subnet_groups().get("DBSubnetGroups", []):
        if g["DBSubnetGroupName"].startswith(PREFIX):
            left.append(f"DB subnet group {g['DBSubnetGroupName']}")
    for s in rds.describe_db_cluster_snapshots().get("DBClusterSnapshots", []):
        if s.get("DBClusterIdentifier", "").startswith(PREFIX):
            left.append(f"cluster snapshot {s['DBClusterSnapshotIdentifier']} ({s.get('SnapshotType')})")
    return left


def teardown(say=print):
    """Delete every cluster of ours still there (any run). For teardown.py."""
    rds, _ = clients()
    runs = set()
    for c in rds.describe_db_clusters().get("DBClusters", []):
        cid = c["DBClusterIdentifier"]
        if cid.startswith(PREFIX) and cid.endswith("-aurora"):
            runs.add(cid[len(PREFIX):-len("-aurora")])
    for i in rds.describe_db_instances().get("DBInstances", []):
        iid = i["DBInstanceIdentifier"]
        if iid.startswith(PREFIX) and iid.endswith("-aurora-1"):
            runs.add(iid[len(PREFIX):-len("-aurora-1")])
    for g in rds.describe_db_subnet_groups().get("DBSubnetGroups", []):
        gid = g["DBSubnetGroupName"]
        if gid.startswith(PREFIX) and gid.endswith("-aurora-subnets"):
            runs.add(gid[len(PREFIX):-len("-aurora-subnets")])
    for r in sorted(runs):
        say(f"aurora teardown: run {r}")
        delete(r, say=say)
    return sorted(runs)


def policy(account="<ACCOUNT_ID>"):
    """The IAM policy for the role fenvix-gha-tests, as the founder adds it (coordinator, 2026-11-10: inline policy
    fenvix-bench-aurora, scoped by NAME, not by tag): create, tag and delete only RDS resources named fenvix-coldread-*
    (this workflow's are fenvix-coldread-s9-<run>-aurora*); read-only Describe calls on every RDS resource (RDS has no
    resource-level limit for them); the RDS service-linked role, once per account. Two lines beyond the founder's first
    version, both read-only: rds:DescribeDBClusterSnapshots (the final check must see that no snapshot is left) and
    rds:ListTagsForResource. The account id is a placeholder: the repository never holds it."""
    a = account
    ours = [f"arn:aws:rds:{REGION}:{a}:cluster:fenvix-coldread-*", f"arn:aws:rds:{REGION}:{a}:db:fenvix-coldread-*",
            f"arn:aws:rds:{REGION}:{a}:subgrp:fenvix-coldread-*"]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "AuroraBenchOwnResources", "Effect": "Allow",
             "Action": ["rds:CreateDBCluster", "rds:CreateDBInstance", "rds:DeleteDBCluster", "rds:DeleteDBInstance",
                        "rds:CreateDBSubnetGroup", "rds:DeleteDBSubnetGroup", "rds:AddTagsToResource"],
             "Resource": ours},
            {"Sid": "AuroraBenchRead", "Effect": "Allow",
             "Action": ["rds:DescribeDBClusters", "rds:DescribeDBInstances", "rds:DescribeDBSubnetGroups",
                        "rds:DescribeDBEngineVersions", "rds:DescribeOrderableDBInstanceOptions",
                        "rds:DescribeDBClusterSnapshots", "rds:ListTagsForResource"],
             "Resource": "*"},
            {"Sid": "RdsServiceLinkedRole", "Effect": "Allow", "Action": "iam:CreateServiceLinkedRole",
             "Resource": f"arn:aws:iam::{a}:role/aws-service-role/rds.amazonaws.com/AWSServiceRoleForRDS",
             "Condition": {"StringLike": {"iam:AWSServiceName": "rds.amazonaws.com"}}},
        ],
    }


def name_ok(name, kind):
    """RDS identifier rules (DECLARED, RDS API reference): 1-63 letters, digits or hyphens, a letter first, no hyphen at
    the end, no two hyphens in a row (subnet group names up to 255); and the policy's prefix fenvix-coldread-."""
    import re
    limit = 255 if kind == "subnets" else 63
    return (name.startswith("fenvix-coldread-") and len(name) <= limit and re.match(r"^[a-z][a-z0-9-]*$", name) is not None
            and not name.endswith("-") and "--" not in name)


def permissions(run):
    """$0: every call the Aurora phase makes, without creating a cluster. A dry check of the names the round would use
    (RDS identifier rules and the policy's prefix fenvix-coldread-); the Describe calls for real, including that
    db.r7g.large is offered for the engine version in eu-west-1a; the DB subnet group created and deleted for real (free,
    tagged); CreateDBCluster and CreateDBInstance sent so that RDS must refuse them after the permission check: they name
    a DB subnet group and a cluster that do not exist (checked first), so "not found" proves the permission and
    "AccessDenied" its absence (an AccessDenied naming a default parameter or option group means the policy needs that
    ARN too); DeleteDBCluster / DeleteDBInstance on names of ours that do not exist, the same way. If a probe ever made a
    cluster (AWS refuses it; moto, the offline imitation, does not), it is deleted at once and reported."""
    from botocore.exceptions import ClientError
    rds, ec2 = clients()
    missing, lines = [], []

    def ok(name, fn, good=()):
        try:
            fn()
            lines.append(f"OK    {name}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in good:
                lines.append(f"OK    {name} (refused after the permission check: {code})")
            else:
                lines.append(f"MANCA {name}: {code}: {str(e)[:200]}")
                missing.append(name)

    n = names(f"perm-{run}")
    # a dry check of the names this round would use (the policy allows fenvix-coldread-* only)
    real = names(run)
    for kind, name in sorted(real.items()):
        if name_ok(name, kind):
            lines.append(f"OK    name {name} ({kind}: RDS rules and the policy's prefix)")
        else:
            lines.append(f"MANCA name {name} ({kind}) breaks the RDS identifier rules or the policy's prefix")
            missing.append(f"name {kind}")
    ok("rds:DescribeDBClusters", lambda: rds.describe_db_clusters(MaxRecords=20))
    ok("rds:DescribeDBInstances", lambda: rds.describe_db_instances(MaxRecords=20))
    ok("rds:DescribeDBSubnetGroups", lambda: rds.describe_db_subnet_groups(MaxRecords=20))
    ok("rds:DescribeDBClusterSnapshots", lambda: rds.describe_db_cluster_snapshots(MaxRecords=20))
    ver = None
    try:
        ver = engine_version(rds)
        lines.append(f"OK    rds:DescribeDBEngineVersions (Aurora PostgreSQL {ver})")
    except Exception as e:
        lines.append(f"MANCA rds:DescribeDBEngineVersions: {str(e)[:200]}")
        missing.append("rds:DescribeDBEngineVersions")
    if ver:
        try:
            zones = sorted({z["Name"] for o in rds.describe_orderable_db_instance_options(Engine=ENGINE, EngineVersion=ver, DBInstanceClass=CLASS)["OrderableDBInstanceOptions"]
                            for z in o.get("AvailabilityZones", [])})
            if f"{REGION}a" in zones or not zones:
                lines.append(f"OK    rds:DescribeOrderableDBInstanceOptions ({CLASS} for Aurora PostgreSQL {ver}: {', '.join(zones) or 'zones not listed'})")
            else:
                lines.append(f"MANCA {CLASS} is not offered in {REGION}a for Aurora PostgreSQL {ver} (zones: {zones})")
                missing.append("instance class")
        except ClientError as e:
            lines.append(f"MANCA rds:DescribeOrderableDBInstanceOptions: {e.response.get('Error', {}).get('Code', '')}")
            missing.append("rds:DescribeOrderableDBInstanceOptions")
    try:
        _vpc, subnets, sg = default_network(ec2)
    except Exception as e:
        lines.append(f"MANCA default VPC, subnets, security group: {str(e)[:200]}")
        return lines, missing + ["default network"]
    ok("rds:CreateDBSubnetGroup with the tag", lambda: rds.create_db_subnet_group(
        DBSubnetGroupName=n["subnets"], DBSubnetGroupDescription="step9 permission check", SubnetIds=subnets, Tags=tags(n["subnets"])))
    ok("rds:DeleteDBSubnetGroup (ours, tagged)", lambda: rds.delete_db_subnet_group(DBSubnetGroupName=n["subnets"]), good=("DBSubnetGroupNotFoundFault",))
    ghost = f"{PREFIX}perm-{run}-absent"
    exists = True
    try:
        rds.describe_db_subnet_groups(DBSubnetGroupName=ghost)
    except ClientError:
        exists = False
    if exists:
        lines.append(f"MANCA a DB subnet group named {ghost} exists: the check will not send CreateDBCluster")
        missing.append("probe")
    else:
        ok(f"rds:CreateDBCluster with the tag (refused: subnet group {ghost} does not exist)", lambda: rds.create_db_cluster(
            DBClusterIdentifier=n["cluster"], Engine=ENGINE, EngineVersion=ver or "16.6",
            MasterUsername="app_user", MasterUserPassword="x" * 24, DatabaseName="app_demo", DBSubnetGroupName=ghost,
            VpcSecurityGroupIds=[sg], StorageEncrypted=True, BackupRetentionPeriod=1, Tags=tags(n["cluster"])),
            good=("DBSubnetGroupNotFoundFault",))
    def cluster_exists():
        try:
            rds.describe_db_clusters(DBClusterIdentifier=n["cluster"])
            return True
        except ClientError:
            return False
    if cluster_exists():
        # safety net: AWS refuses a cluster whose subnet group does not exist; if a cluster exists anyway, it goes now
        lines.append(f"MANCA the CreateDBCluster probe made a cluster ({n['cluster']}): AWS did not refuse it; deleted at once")
        missing.append("probe")
        rds.delete_db_cluster(DBClusterIdentifier=n["cluster"], SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
    if cluster_exists():
        lines.append(f"MANCA cluster {n['cluster']} still exists: the CreateDBInstance probe is not sent")
        missing.append("probe")
    else:
        ok(f"rds:CreateDBInstance {CLASS} with the tag (refused: cluster {n['cluster']} does not exist)", lambda: rds.create_db_instance(
            DBInstanceIdentifier=n["instance"], DBClusterIdentifier=n["cluster"], Engine=ENGINE, DBInstanceClass=CLASS,
            AvailabilityZone=f"{REGION}a", PubliclyAccessible=False, Tags=tags(n["instance"])),
            good=("DBClusterNotFoundFault", "DBClusterNotFound"))
    ok("rds:DeleteDBInstance (ours)", lambda: rds.delete_db_instance(DBInstanceIdentifier=n["instance"]),
       good=("DBInstanceNotFound", "DBInstanceNotFoundFault"))
    ok("rds:DeleteDBCluster (ours, no final snapshot)", lambda: rds.delete_db_cluster(DBClusterIdentifier=n["cluster"], SkipFinalSnapshot=True, DeleteAutomatedBackups=True),
       good=("DBClusterNotFoundFault", "DBClusterNotFound"))
    left = [x for x in leftovers() if f"perm-{run}" in x]
    lines.append("OK    nothing left by the check" if not left else f"MANCA left by the check: {left}")
    if left:
        missing.append("leftovers")
    return lines, missing


if __name__ == "__main__":
    if sys.argv[1:2] == ["policy"]:
        print(json.dumps(policy(), indent=2))
    elif sys.argv[1:2] == ["permissions"]:
        lines, missing = permissions(sys.argv[2] if len(sys.argv) > 2 else os.environ.get("GITHUB_RUN_ID", "0"))
        print("\n".join(lines))
        print("AURORA PERMESSI: tutti presenti" if not missing else f"AURORA PERMESSI MANCANTI: {', '.join(missing)}")
        sys.exit(1 if missing else 0)
    else:
        print(__doc__)
        sys.exit(2)
