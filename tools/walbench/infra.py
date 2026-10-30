#!/usr/bin/env python3
"""walbench AWS resources: create, delete, verify. eu-west-1 only. Everything is tagged project=fenvix-walbench and
named fenvix-walbench-<run>..., so `teardown --all` and `verify` find every resource of every run by tag AND by name,
even without a state file.

  infra.py prices                 fetch the current eu-west-1 prices (public offer files + Price List API) -> prices.json
  infra.py create  --run R        buckets, directory bucket, table, security group, EBS volume (instances: orchestrate.py)
  infra.py teardown [--run R|--all]   terminate instances, delete volumes, security group, buckets (emptied), table
  infra.py verify                 list anything left (tag API + name listings); exit 1 if anything is left

Errors are reduced to their AWS error code (no account id or ARN is printed). No credentials are read or printed here:
boto3 takes them from the environment (the OIDC role session of the workflow)."""
import argparse
import json
import os
import sys
import time
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "eu-west-1"
TAG_KEY, TAG_VAL = "project", "fenvix-walbench"
PREFIX = "fenvix-walbench-"
INSTANCE_TYPE = "c6i.large"
PROFILE = os.environ.get("WALBENCH_INSTANCE_PROFILE", "fenvix-walbench-ec2")
EXPRESS_AZ_IDS = ["euw1-az1", "euw1-az3", "euw1-az2"]  # tried in this order; the first that accepts a directory bucket wins
UBUNTU_OWNER = "099720109477"  # Canonical
UBUNTU_NAME = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
CFG = Config(region_name=REGION, retries={"max_attempts": 8, "mode": "standard"})
OUT = os.environ.get("WALBENCH_OUT", "results-walbench")


def c(svc, region=REGION):
    # a new session each time, so credentials renewed in the environment are picked up
    return boto3.session.Session().client(svc, config=CFG.merge(Config(region_name=region)))


def code(e):
    if isinstance(e, ClientError):
        return e.response.get("Error", {}).get("Code", "ClientError")
    return type(e).__name__


def tags(run, role=None):
    t = [{"Key": TAG_KEY, "Value": TAG_VAL}, {"Key": "walbench-run", "Value": run}, {"Key": "Name", "Value": f"{PREFIX}{run}" + (f"-{role}" if role else "")}]
    if role:
        t.append({"Key": "walbench-role", "Value": role})
    return t


def log(msg):
    print(time.strftime("%H:%M:%S ") + msg, flush=True)


# ---------------------------------------------------------------------------------------------------------- prices
OFFER = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{svc}/current/eu-west-1/index.json"
PAGES = {
    "s3": "https://aws.amazon.com/s3/pricing/",
    "express": "https://aws.amazon.com/s3/storage-classes/express-one-zone/ (prices on https://aws.amazon.com/s3/pricing/)",
    "dynamodb": "https://aws.amazon.com/dynamodb/pricing/on-demand/",
    "ec2": "https://aws.amazon.com/ec2/pricing/on-demand/",
    "ebs": "https://aws.amazon.com/ebs/pricing/",
    "vpc_ipv4": "https://aws.amazon.com/vpc/pricing/",
    "data_transfer": "https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer_within_the_same_AWS_Region",
}


def _offer_prices(svc, wanted):
    """wanted: {name: usagetype}. Returns {name: {usd, unit, description, usagetype, sku}} from the public offer file."""
    url = OFFER.format(svc=svc)
    d = json.load(urllib.request.urlopen(url, timeout=120))
    od = d["terms"]["OnDemand"]
    out = {"_source": {"url": url, "publicationDate": d.get("publicationDate"), "version": d.get("version")}}
    for name, ut in wanted.items():
        hits = []
        for sku, p in d["products"].items():
            if p.get("attributes", {}).get("usagetype") != ut:
                continue
            for t in od.get(sku, {}).values():
                for pd in t["priceDimensions"].values():
                    hits.append({"usd": float(pd["pricePerUnit"]["USD"]), "unit": pd["unit"], "description": pd["description"],
                                 "usagetype": ut, "sku": sku, "beginRange": pd.get("beginRange"), "effectiveDate": t.get("effectiveDate")})
        # tiered prices (storage): keep the first paid tier, i.e. the highest price
        hits.sort(key=lambda h: -h["usd"])
        if not hits:
            raise SystemExit(f"price not found: {svc} {ut}")
        out[name] = hits[0]
        out[name]["all_tiers"] = len(hits)
    return out


def _api_price(filters, service):
    pr = c("pricing", "us-east-1")
    r = pr.get_products(ServiceCode=service, Filters=[{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in filters.items()], MaxResults=20)
    hits = []
    for s in r["PriceList"]:
        p = json.loads(s)
        for t in p["terms"].get("OnDemand", {}).values():
            for pd in t["priceDimensions"].values():
                usd = float(pd["pricePerUnit"].get("USD", 0))
                if usd > 0:
                    hits.append({"usd": usd, "unit": pd["unit"], "description": pd["description"], "sku": p["product"]["sku"],
                                 "usagetype": p["product"]["attributes"].get("usagetype"), "effectiveDate": t.get("effectiveDate"),
                                 "publicationDate": p.get("publicationDate")})
    if len(hits) != 1:
        raise SystemExit(f"expected exactly one price for {service} {filters}, got {len(hits)}: {[h['description'] for h in hits]}")
    hits[0]["source"] = f"AWS Price List Query API pricing:GetProducts ServiceCode={service} filters={filters}"
    return hits[0]


def prices():
    p = {"fetched_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "region": REGION, "pages": PAGES}
    p["s3"] = _offer_prices("AmazonS3", {"std_put": "EU-Requests-Tier1", "std_get": "EU-Requests-Tier2", "std_storage": "EU-TimedStorage-ByteHrs",
                                          "xz_put": "EU-Requests-XZ-Tier1", "xz_get": "EU-Requests-XZ-Tier2", "xz_upload_gb": "EU-Upload-XZ",
                                          "xz_retrieval_gb": "EU-Retrieval-XZ", "xz_storage": "EU-TimedStorage-XZ-ByteHrs"})
    p["dynamodb"] = _offer_prices("AmazonDynamoDB", {"wru": "EU-WriteRequestUnits", "rru": "EU-ReadRequestUnits", "storage": "EU-TimedStorage-ByteHrs"})
    p["vpc"] = _offer_prices("AmazonVPC", {"ipv4_in_use_hour": "EU-PublicIPv4:InUseAddress"})
    p["transfer"] = _offer_prices("AWSDataTransfer", {"inter_az_gb": "EU-DataTransfer-Regional-Bytes"})
    # EC2 and EBS: the regional EC2 offer file is ~440 MB, so these two come from the Price List Query API
    if os.environ.get("WALBENCH_TEST_NO_EXPRESS"):  # local emulator test only (no Price List API there)
        p["ec2"] = {"c6i_large_hour": {"usd": 0.2, "TEST_STUB": True}, "gp3_gb_month": {"usd": 0.09, "TEST_STUB": True}}
        return p
    p["ec2"] = {"c6i_large_hour": _api_price({"regionCode": REGION, "instanceType": INSTANCE_TYPE, "operatingSystem": "Linux", "tenancy": "Shared",
                                              "preInstalledSw": "NA", "capacitystatus": "Used", "licenseModel": "No License required"}, "AmazonEC2"),
                "gp3_gb_month": _api_price({"regionCode": REGION, "volumeApiName": "gp3", "productFamily": "Storage"}, "AmazonEC2")}
    return p


def flat_prices(p):
    """The numbers the cost code uses, USD."""
    return {
        "s3_put": p["s3"]["std_put"]["usd"], "s3_get": p["s3"]["std_get"]["usd"], "s3_gb_month": p["s3"]["std_storage"]["usd"],
        "xz_put": p["s3"]["xz_put"]["usd"], "xz_get": p["s3"]["xz_get"]["usd"], "xz_upload_gb": p["s3"]["xz_upload_gb"]["usd"],
        "xz_retrieval_gb": p["s3"]["xz_retrieval_gb"]["usd"], "xz_gb_month": p["s3"]["xz_storage"]["usd"],
        "ddb_wru": p["dynamodb"]["wru"]["usd"], "ddb_rru": p["dynamodb"]["rru"]["usd"], "ddb_gb_month": p["dynamodb"]["storage"]["usd"],
        "ipv4_hour": p["vpc"]["ipv4_in_use_hour"]["usd"], "inter_az_gb": p["transfer"]["inter_az_gb"]["usd"],
        "ec2_hour": p["ec2"]["c6i_large_hour"]["usd"], "gp3_gb_month": p["ec2"]["gp3_gb_month"]["usd"],
    }


# ---------------------------------------------------------------------------------------------------------- create
def names(run):
    return {"bucket": f"{PREFIX}{run}", "table": f"{PREFIX}{run}", "sg": f"{PREFIX}{run}"}


def default_network():
    ec2 = c("ec2")
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise SystemExit("no default VPC in eu-west-1: this benchmark uses the default VPC (see docs/walbench-permissions.md)")
    vpc = vpcs[0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc]}, {"Name": "default-for-az", "Values": ["true"]}])["Subnets"]
    by_zone_id = {s["AvailabilityZoneId"]: {"subnet": s["SubnetId"], "az": s["AvailabilityZone"], "az_id": s["AvailabilityZoneId"]} for s in subnets}
    return vpc, by_zone_id


def create(run, st):
    """Creates the shared resources; every step is recorded in st (the state dict) before the next starts."""
    n = names(run)
    s3 = c("s3")
    ec2 = c("ec2")
    vpc, zones = default_network()
    st["vpc"] = vpc
    # 1. S3 standard bucket (backend 1 + run files + coordination), lifecycle: everything expires after 2 days anyway
    s3.create_bucket(Bucket=n["bucket"], CreateBucketConfiguration={"LocationConstraint": REGION})
    st["bucket"] = n["bucket"]
    s3.put_bucket_tagging(Bucket=n["bucket"], Tagging={"TagSet": tags(run)[:2]})
    s3.put_bucket_lifecycle_configuration(Bucket=n["bucket"], LifecycleConfiguration={"Rules": [{
        "ID": "expire-2-days", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": {"Days": 2},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]})
    log(f"bucket {n['bucket']} created")
    # 2. S3 Express One Zone directory bucket, in an AZ that has a default subnet (the client goes in that AZ)
    st["express_errors"] = {}
    for az_id in ([] if os.environ.get("WALBENCH_TEST_NO_EXPRESS") else EXPRESS_AZ_IDS):  # (local emulator test only)
        if az_id not in zones:
            continue
        name = f"{PREFIX}{run}--{az_id}--x-s3"
        try:
            s3.create_bucket(Bucket=name, CreateBucketConfiguration={
                "Location": {"Type": "AvailabilityZone", "Name": az_id}, "Bucket": {"DataRedundancy": "SingleAvailabilityZone", "Type": "Directory"}})
            st["express_bucket"], st["express_az_id"] = name, az_id
            break
        except ClientError as e:
            st["express_errors"][az_id] = code(e)
    if "express_bucket" not in st:
        if not os.environ.get("WALBENCH_TEST_NO_EXPRESS"):
            raise SystemExit(f"no directory bucket could be created: {st['express_errors']}")
        st["express_bucket"], st["express_az_id"] = "none--x-s3", sorted(zones)[0]
    try:  # tags on a directory bucket (S3 Control TagResource); the name prefix finds it in any case
        if os.environ.get("WALBENCH_TEST_NO_EXPRESS"):
            raise RuntimeError("test")
        acct = c("sts").get_caller_identity()["Account"]
        arn = f"arn:aws:s3express:{REGION}:{acct}:bucket/{st['express_bucket']}"
        c("s3control").tag_resource(AccountId=acct, ResourceArn=arn, Tags=[{"Key": t["Key"], "Value": t["Value"]} for t in tags(run)[:2]])
        st["express_tagged"] = True
    except Exception as e:
        st["express_tagged"] = f"not tagged ({code(e)}); found by name prefix"
    log(f"directory bucket {st['express_bucket']} created (AZ id {st['express_az_id']})")
    client_zone = zones[st["express_az_id"]]
    others = [z for zid, z in sorted(zones.items()) if zid != st["express_az_id"]]
    if len(others) < 2:
        raise SystemExit("need default subnets in 3 AZs")
    st["zones"] = {"client": client_zone, "node0": client_zone, "node1": others[0], "node2": others[1], "standby": client_zone}
    # 3. DynamoDB table, on-demand
    ddb = c("dynamodb")
    ddb.create_table(TableName=n["table"], BillingMode="PAY_PER_REQUEST",
                     AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}],
                     KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                     Tags=tags(run)[:2])
    st["table"] = n["table"]
    ddb.get_waiter("table_exists").wait(TableName=n["table"])
    log(f"table {n['table']} active")
    # 4. security group: quorum port 7700 open only to members of the same group; no inbound from the internet
    sg = ec2.create_security_group(GroupName=n["sg"], Description="fenvix walbench (quorum port within the group only)", VpcId=vpc,
                                   TagSpecifications=[{"ResourceType": "security-group", "Tags": tags(run)}])["GroupId"]
    st["sg"] = sg
    ec2.authorize_security_group_ingress(GroupId=sg, IpPermissions=[{"IpProtocol": "tcp", "FromPort": 7700, "ToPort": 7700, "UserIdGroupPairs": [{"GroupId": sg}]}])
    log("security group created")
    # 5. EBS gp3 log volume (baseline 3000 IOPS / 125 MB/s, no extra charge), in the client's AZ
    vol = ec2.create_volume(AvailabilityZone=client_zone["az"], Size=20, VolumeType="gp3", Iops=3000, Throughput=125,
                            TagSpecifications=[{"ResourceType": "volume", "Tags": tags(run, "log-volume")}])["VolumeId"]
    st["volume"] = vol
    ec2.get_waiter("volume_available").wait(VolumeIds=[vol])
    log("log volume created")
    # 6. AMI: latest Canonical Ubuntu 24.04 amd64 (same glibc as the GitHub runner that builds the binary)
    imgs = ec2.describe_images(Owners=[UBUNTU_OWNER], Filters=[{"Name": "name", "Values": [UBUNTU_NAME]}, {"Name": "state", "Values": ["available"]}])["Images"]
    if not imgs and os.environ.get("WALBENCH_TEST_NO_EXPRESS"):  # (local emulator test only)
        imgs = ec2.describe_images()["Images"][:1]
    if not imgs:
        raise SystemExit("Ubuntu 24.04 AMI not found")
    st["ami"] = sorted(imgs, key=lambda i: i["CreationDate"])[-1]["ImageId"]
    st["ami_name"] = sorted(imgs, key=lambda i: i["CreationDate"])[-1]["Name"]
    return st


def launch(run, st, role, user_data):
    ec2 = c("ec2")
    z = st["zones"][role]
    r = ec2.run_instances(
        ImageId=st["ami"], InstanceType=INSTANCE_TYPE, MinCount=1, MaxCount=1, SubnetId=z["subnet"], SecurityGroupIds=[st["sg"]],
        IamInstanceProfile={"Name": PROFILE}, UserData=user_data, InstanceInitiatedShutdownBehavior="terminate",
        MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
        BlockDeviceMappings=[{"DeviceName": "/dev/sda1", "Ebs": {"VolumeSize": 10, "VolumeType": "gp3", "DeleteOnTermination": True}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": tags(run, role)}, {"ResourceType": "volume", "Tags": tags(run, role + "-root")},
                           {"ResourceType": "network-interface", "Tags": tags(run, role)}])
    iid = r["Instances"][0]["InstanceId"]
    st.setdefault("instances", {})[role] = iid
    log(f"instance {role} launched in {z['az_id']}")
    return iid


# -------------------------------------------------------------------------------------------------------- teardown
def _empty_bucket(s3, b, directory):
    n = 0
    tok = None
    while True:
        kw = {"Bucket": b}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        keys = [{"Key": o["Key"]} for o in r.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=b, Delete={"Objects": keys[i:i + 1000], "Quiet": True})
            n += len(keys[i:i + 1000])
        tok = r.get("NextContinuationToken")
        if not r.get("IsTruncated") or not tok:
            # a directory bucket may need another pass after deletes; loop until a listing is empty
            if not keys:
                break
            tok = None
    if not directory:
        for u in s3.list_multipart_uploads(Bucket=b).get("Uploads", []):
            s3.abort_multipart_upload(Bucket=b, Key=u["Key"], UploadId=u["UploadId"])
    return n


def teardown(run=None):
    """Deletes every walbench resource (of `run`, or of every run when run is None). Never raises: each step is
    retried and its outcome recorded; the verify step decides."""
    steps = []

    def step(name, fn, tries=3):
        for i in range(tries):
            try:
                r = fn()
                steps.append({"step": name, "ok": True, "result": r})
                log(f"teardown {name}: ok {r if r is not None else ''}")
                return r
            except Exception as e:
                err = code(e)
                if i + 1 == tries:
                    steps.append({"step": name, "ok": False, "error": err})
                    log(f"teardown {name}: FAILED {err}")
                    return None
                time.sleep(10 * (i + 1))

    ec2, s3, ddb = c("ec2"), c("s3"), c("dynamodb")
    flt = [{"Name": f"tag:{TAG_KEY}", "Values": [TAG_VAL]}] + ([{"Name": "tag:walbench-run", "Values": [run]}] if run else [])

    def instances():
        r = ec2.describe_instances(Filters=flt + [{"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped", "shutting-down"]}])
        ids = [i["InstanceId"] for res in r["Reservations"] for i in res["Instances"]]
        if ids:
            ec2.terminate_instances(InstanceIds=ids)
            ec2.get_waiter("instance_terminated").wait(InstanceIds=ids, WaiterConfig={"Delay": 10, "MaxAttempts": 60})
        return {"terminated": ids}
    step("terminate-instances", instances)

    def volumes():
        vols = ec2.describe_volumes(Filters=flt)["Volumes"]
        done = []
        for v in vols:
            for a in v.get("Attachments", []):
                if a["State"] in ("attached", "attaching"):
                    ec2.detach_volume(VolumeId=v["VolumeId"], Force=True)
            if v.get("Attachments"):
                ec2.get_waiter("volume_available").wait(VolumeIds=[v["VolumeId"]], WaiterConfig={"Delay": 5, "MaxAttempts": 60})
            ec2.delete_volume(VolumeId=v["VolumeId"])
            done.append(v["VolumeId"])
        return {"deleted": done}
    step("delete-volumes", volumes)

    def sgs():
        out = []
        for g in ec2.describe_security_groups(Filters=flt)["SecurityGroups"]:
            for i in range(18):  # network interfaces of terminated instances can take a few minutes to go
                try:
                    ec2.delete_security_group(GroupId=g["GroupId"])
                    out.append(g["GroupId"])
                    break
                except ClientError as e:
                    if code(e) != "DependencyViolation" or i == 17:
                        raise
                    time.sleep(10)
        return {"deleted": out}
    step("delete-security-groups", sgs)

    def buckets():
        out = []
        for b in s3.list_buckets().get("Buckets", []):
            nm = b["Name"]
            if nm.startswith(PREFIX + (run or "")) and not nm.endswith("--x-s3"):
                n = _empty_bucket(s3, nm, False)
                s3.delete_bucket(Bucket=nm)
                out.append({"bucket": nm, "objects_deleted": n})
        return out
    step("delete-s3-buckets", buckets)

    def dir_buckets():
        out = []
        r = s3.list_directory_buckets()
        for b in r.get("Buckets", []):
            nm = b["Name"]
            if nm.startswith(PREFIX + (run or "")):
                n = _empty_bucket(s3, nm, True)
                s3.delete_bucket(Bucket=nm)
                out.append({"bucket": nm, "objects_deleted": n})
        return out
    step("delete-directory-buckets", dir_buckets)

    def tables():
        out = []
        for page in ddb.get_paginator("list_tables").paginate():
            for t in page["TableNames"]:
                if t.startswith(PREFIX + (run or "")):
                    ddb.delete_table(TableName=t)
                    ddb.get_waiter("table_not_exists").wait(TableName=t)
                    out.append(t)
        return out
    step("delete-tables", tables)
    return steps


def verify():
    """Everything walbench could have created, looked up two ways (tag API and per-service listings by name/tag)."""
    left = []
    ec2, s3, ddb = c("ec2"), c("s3"), c("dynamodb")
    flt = [{"Name": f"tag:{TAG_KEY}", "Values": [TAG_VAL]}]
    for res in ec2.describe_instances(Filters=flt)["Reservations"]:
        for i in res["Instances"]:
            if i["State"]["Name"] != "terminated":
                left.append({"type": "instance", "id": i["InstanceId"], "state": i["State"]["Name"]})
    left += [{"type": "volume", "id": v["VolumeId"]} for v in ec2.describe_volumes(Filters=flt)["Volumes"]]
    left += [{"type": "security-group", "id": g["GroupId"]} for g in ec2.describe_security_groups(Filters=flt)["SecurityGroups"]]
    left += [{"type": "network-interface", "id": n["NetworkInterfaceId"]} for n in ec2.describe_network_interfaces(Filters=flt)["NetworkInterfaces"]]
    left += [{"type": "s3-bucket", "id": b["Name"]} for b in s3.list_buckets().get("Buckets", []) if b["Name"].startswith(PREFIX)]
    left += [{"type": "directory-bucket", "id": b["Name"]} for b in s3.list_directory_buckets().get("Buckets", []) if b["Name"].startswith(PREFIX)]
    for page in ddb.get_paginator("list_tables").paginate():
        left += [{"type": "dynamodb-table", "id": t} for t in page["TableNames"] if t.startswith(PREFIX)]
    # the tag API (Resource Groups Tagging) as a second, independent listing; terminated instances are ignored
    tagged = []
    for page in c("resourcegroupstaggingapi").get_paginator("get_resources").paginate(TagFilters=[{"Key": TAG_KEY, "Values": [TAG_VAL]}]):
        for r in page["ResourceTagMappingList"]:
            arn = r["ResourceARN"]
            kind = arn.split(":")[2] + ":" + arn.split(":")[5].split("/")[0]
            rid = arn.split("/")[-1].split(":")[-1]
            tagged.append({"kind": kind, "id": rid})
    still = []
    for t in tagged:
        if t["kind"] == "ec2:instance":
            try:
                st = ec2.describe_instances(InstanceIds=[t["id"]])["Reservations"][0]["Instances"][0]["State"]["Name"]
                if st == "terminated":
                    continue
            except ClientError:
                continue
        if t["kind"] == "ec2:security-group-rule":
            continue  # goes with its group (already listed above if the group exists)
        if any(x["id"] == t["id"] for x in left):
            continue
        still.append(t)
    return {"left": left, "tag_api_extra": still, "clean": not left and not still,
            "checked_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["prices", "create", "teardown", "verify"])
    ap.add_argument("--run")
    ap.add_argument("--all", action="store_true")
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    if a.action == "prices":
        p = prices()
        json.dump(p, open(f"{OUT}/prices.json", "w"), indent=1)
        print(json.dumps(flat_prices(p), indent=1))
    elif a.action == "create":
        st = {"run": a.run}
        try:
            create(a.run, st)
        finally:
            json.dump(st, open(f"{OUT}/state.json", "w"), indent=1)
    elif a.action == "teardown":
        if not a.run and not a.all:
            sys.exit("--run R or --all")
        steps = teardown(None if a.all else a.run)
        v = verify()
        json.dump({"steps": steps, "verify": v}, open(f"{OUT}/teardown.json", "w"), indent=1)
        print(json.dumps(v, indent=1))
        sys.exit(0 if v["clean"] else 1)
    else:
        v = verify()
        print(json.dumps(v, indent=1))
        sys.exit(0 if v["clean"] else 1)


if __name__ == "__main__":
    main()
