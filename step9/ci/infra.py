"""Step 9 · where the machines and the buckets live. Two implementations of the same small interface, so the runner
(run.py), the teardown and the final check are the same code in both modes:
  AwsPlatform    EC2 machines and S3 buckets in eu-west-1 (the AWS round, from GitHub Actions with OIDC credentials);
  LocalPlatform  docker containers on a docker network, and MinIO as S3 (the $0 validation, step9/local/).
Everything this code creates carries the tag project=fenvix-coldread (AWS: the tag the founder's IAM policies already
allow; local: a docker label), and every bucket name starts with fenvix-coldread-: teardown and the final check look
for exactly these, so they also find what an interrupted run left behind.
Interface:
  setup(run)                      network: security groups (AWS) or a docker network (local)
  new_bucket(name, days)          private bucket, expires after `days`
  launch(name, role, mtype, az, user_data) -> (id, private_ip)
  alive() -> [(id, type)]         tagged machines not terminated
  terminate(ids)
  presign(bucket, key, seconds)   a link the machine downloads its package from
  leftovers() -> [str]            what is left that costs money (machines, disks, addresses, buckets, groups/networks)
  cleanup()                       delete everything tagged (machines first)
"""
import json
import os
import subprocess
import time

import boto3
from botocore.config import Config

TAG = "fenvix-coldread"
PREFIX = "fenvix-coldread-"
REGION = "eu-west-1"
ZONES = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]


def s3_client():
    # AWS_ENDPOINT_URL (local mode only) points boto3 at MinIO
    return boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}))


class Buckets:
    """S3 part, the same for AWS and MinIO."""

    def __init__(self):
        self.s3 = s3_client()

    def new_bucket(self, name, days=1):
        if not (name.startswith(PREFIX) or name.startswith("fenvix-kitchen-pantry-")):
            raise ValueError(f"bucket name not allowed: {name}")   # only the prefixes the IAM policies cover
        self.s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": REGION})
        if not self.local:
            self.s3.put_public_access_block(Bucket=name, PublicAccessBlockConfiguration={
                "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
            self.s3.put_bucket_tagging(Bucket=name, Tagging={"TagSet": [{"Key": "project", "Value": TAG}]})
        self.s3.put_bucket_lifecycle_configuration(Bucket=name, LifecycleConfiguration={"Rules": [{
            "ID": "expire", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": {"Days": days},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]})

    def buckets(self):
        return [b["Name"] for b in self.s3.list_buckets().get("Buckets", []) if b["Name"].startswith(PREFIX)]

    def delete_bucket(self, name):
        pag = self.s3.get_paginator("list_object_versions") if not self.local else None
        keys = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=name):
            keys += [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if pag:
            for page in pag.paginate(Bucket=name):
                keys += [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("Versions", []) + page.get("DeleteMarkers", [])]
        for i in range(0, len(keys), 1000):
            self.s3.delete_objects(Bucket=name, Delete={"Objects": keys[i:i + 1000], "Quiet": True})
        self.s3.delete_bucket(Bucket=name)

    def presign(self, bucket, key, seconds=21600):
        return self.s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=seconds)


class AwsPlatform(Buckets):
    local = False

    def __init__(self, machine_role="fenvix-coldread-machine", disk_gb=20):
        super().__init__()
        self.ec2 = boto3.client("ec2", region_name=REGION)
        self.ssm = boto3.client("ssm", region_name=REGION)
        self.machine_role, self.disk_gb = machine_role, disk_gb
        self.subnet, self.group, self.ami = {}, {}, {}

    def _tags(self, kind, name=None):
        t = [{"Key": "project", "Value": TAG}] + ([{"Key": "Name", "Value": name}] if name else [])
        return [{"ResourceType": kind, "Tags": t}]

    def setup(self, run, network="default-sg"):
        vpc = self.ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
        for s in self.ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc]}, {"Name": "default-for-az", "Values": ["true"]}])["Subnets"]:
            self.subnet[s["AvailabilityZone"]] = s["SubnetId"]
        missing = [z for z in ZONES if z not in self.subnet]
        if missing:
            raise RuntimeError(f"no default subnet in {missing}")
        if network == "default-sg":
            # the default VPC's own group: its members reach each other on every port, nothing from the internet;
            # nothing to create or delete, so no security-group permission is needed (founder: zero new IAM)
            gid = self.ec2.describe_security_groups(Filters=[{"Name": "vpc-id", "Values": [vpc]},
                                                            {"Name": "group-name", "Values": ["default"]}])["SecurityGroups"][0]["GroupId"]
            self.group = {"client": gid, "kitchen": gid}
            for arch, ssm_arch in (("x86_64", "amd64"), ("aarch64", "arm64")):
                self.ami[arch] = self.ssm.get_parameter(
                    Name=f"/aws/service/canonical/ubuntu/server/24.04/stable/current/{ssm_arch}/hvm/ebs-gp3/ami-id")["Parameter"]["Value"]
            return {"subnets": self.subnet, "groups": self.group, "network": "default-sg"}
        for g in ("client", "kitchen"):
            self.group[g] = self.ec2.create_security_group(
                GroupName=f"{PREFIX}s9-{g}-{run}", Description=f"step9 {g} (deleted at the end of the run)", VpcId=vpc,
                TagSpecifications=self._tags("security-group"))["GroupId"]
        # kitchen: 27017-27018 (our server, the takeover pair) and 7200 (log clients) from the client group and from the
        # kitchen itself; 7100 (log replicas between them) only inside the kitchen. No port from the internet.
        k, c = self.group["kitchen"], self.group["client"]
        # (7101/7201: the separate log group of the raw log-ack benchmark, see machine/log.sh)
        perms = [{"IpProtocol": "tcp", "FromPort": 27017, "ToPort": 27018, "UserIdGroupPairs": [{"GroupId": c}, {"GroupId": k}]},
                 {"IpProtocol": "tcp", "FromPort": 7200, "ToPort": 7201, "UserIdGroupPairs": [{"GroupId": c}, {"GroupId": k}]},
                 {"IpProtocol": "tcp", "FromPort": 7100, "ToPort": 7101, "UserIdGroupPairs": [{"GroupId": k}]},
                 {"IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432, "UserIdGroupPairs": [{"GroupId": c}]}]
        self.ec2.authorize_security_group_ingress(GroupId=k, IpPermissions=perms)
        for arch, ssm_arch in (("x86_64", "amd64"), ("aarch64", "arm64")):
            self.ami[arch] = self.ssm.get_parameter(
                Name=f"/aws/service/canonical/ubuntu/server/24.04/stable/current/{ssm_arch}/hvm/ebs-gp3/ami-id")["Parameter"]["Value"]
        return {"subnets": self.subnet, "groups": self.group}

    def launch(self, name, role, mtype, az, user_data, arch, disk_gb=None):
        group = self.group["client" if role in ("client", "filler", "extras", "mongoclient") else "kitchen"]   # pgbox: kitchen (5432 from clients)
        r = self.ec2.run_instances(
            ImageId=self.ami[arch], InstanceType=mtype, MinCount=1, MaxCount=1,
            NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": self.subnet[az], "Groups": [group], "AssociatePublicIpAddress": True}],
            IamInstanceProfile={"Name": self.machine_role}, InstanceInitiatedShutdownBehavior="terminate",
            MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
            BlockDeviceMappings=[{"DeviceName": "/dev/sda1", "Ebs": {"VolumeSize": disk_gb or self.disk_gb, "VolumeType": "gp3", "DeleteOnTermination": True}}],
            TagSpecifications=self._tags("instance", f"{PREFIX}s9-{name}") + self._tags("volume"),
            UserData=user_data)
        i = r["Instances"][0]
        return i["InstanceId"], i.get("PrivateIpAddress")

    def alive(self):
        out = []
        for page in self.ec2.get_paginator("describe_instances").paginate(Filters=[
                {"Name": "tag:project", "Values": [TAG]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped", "shutting-down"]}]):
            for r in page["Reservations"]:
                out += [(i["InstanceId"], i["InstanceType"]) for i in r["Instances"]]
        return out

    def terminate(self, ids):
        if ids:
            self.ec2.terminate_instances(InstanceIds=list(ids))

    def wait_terminated(self, ids):
        if ids:
            self.ec2.get_waiter("instance_terminated").wait(InstanceIds=list(ids), WaiterConfig={"Delay": 10, "MaxAttempts": 60})

    def leftovers(self, since_iso=None):
        left = [f"machine {i} ({t})" for i, t in self.alive()]
        tagf = [{"Name": "tag:project", "Values": [TAG]}]
        left += [f"disk {v['VolumeId']} ({v['State']}, tagged)" for v in self.ec2.describe_volumes(Filters=tagf)["Volumes"]]
        for v in self.ec2.describe_volumes(Filters=[{"Name": "status", "Values": ["available"]}])["Volumes"]:
            if since_iso and v["CreateTime"].strftime("%Y-%m-%dT%H:%M:%SZ") >= since_iso and not any(
                    t["Key"] == "project" for t in v.get("Tags", [])):
                left.append(f"disk {v['VolumeId']} (unattached, created during the run)")
        left += [f"address {a['PublicIp']}" for a in self.ec2.describe_addresses(Filters=tagf)["Addresses"]]
        left += [f"bucket {b}" for b in self.buckets()]
        left += [f"security group {g['GroupId']} ({g['GroupName']})" for g in self.ec2.describe_security_groups(Filters=tagf)["SecurityGroups"]]
        return left

    def cleanup(self, say=print):
        ids = [i for i, _ in self.alive()]
        if ids:
            self.terminate(ids)
            say(f"machines terminated: {' '.join(ids)}")
            self.wait_terminated(ids)
        for b in self.buckets():
            self.delete_bucket(b)
            say(f"bucket deleted: {b}")
        tagf = [{"Name": "tag:project", "Values": [TAG]}]
        for _ in range(18):  # a just-terminated machine keeps its group a few seconds
            groups = self.ec2.describe_security_groups(Filters=tagf)["SecurityGroups"]
            if not groups:
                break
            for g in sorted(groups, key=lambda g: -len(g.get("IpPermissions", []))):
                if g.get("IpPermissions"):
                    try:
                        self.ec2.revoke_security_group_ingress(GroupId=g["GroupId"], IpPermissions=g["IpPermissions"])
                    except Exception:
                        pass
            for g in groups:
                try:
                    self.ec2.delete_security_group(GroupId=g["GroupId"])
                    say(f"security group deleted: {g['GroupId']}")
                except Exception:
                    pass
            time.sleep(10)


class LocalPlatform(Buckets):
    """Docker containers as machines (cpus and memory of the type, same package, same scripts), MinIO as S3."""
    local = True

    def __init__(self, image="fenvix-step9-machine", vcpu_cap=None):
        super().__init__()
        self.image = image
        self.vcpu_cap = vcpu_cap or os.cpu_count()
        self.network = None

    def _docker(self, *args, check=True):
        r = subprocess.run(["docker", *args], capture_output=True, text=True)
        if check and r.returncode != 0:
            raise RuntimeError(f"docker {' '.join(args[:3])}: {r.stderr.strip()[:300]}")
        return r.stdout.strip()

    def setup(self, run, network="default-sg"):
        self.network = f"{PREFIX}s9-{run}"
        self._docker("network", "create", "--label", f"project={TAG}", self.network)
        gw = json.loads(self._docker("network", "inspect", self.network))[0]["IPAM"]["Config"][0]["Gateway"]
        # the containers reach MinIO (on this machine) through the network's gateway
        port = os.environ["AWS_ENDPOINT_URL"].rsplit(":", 1)[1]
        self.machine_endpoint = f"http://{gw}:{port}"
        return {"network": self.network, "machine_s3_endpoint": self.machine_endpoint}

    def launch(self, name, role, mtype, az, user_data, arch, disk_gb=None):
        from machine_types import TYPES
        vcpu, gib = TYPES[mtype][0], TYPES[mtype][1]
        cname = f"{PREFIX}s9-{self.network.rsplit('-', 1)[1]}-{name}"
        env = {"LOCAL_TEST": "1", "FAKE_TYPE": mtype, "FAKE_AZ": az, "AWS_ENDPOINT_URL": self.machine_endpoint,
               "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"], "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
               "AWS_REGION": REGION, "PUSH_S": os.environ.get("PUSH_S", "10"), "SST_EVERY_S": os.environ.get("SST_EVERY_S", "20"),
               "SETTLE_STEP_S": "5", "SETTLE_MAX_MIN": os.environ.get("LOCAL_SETTLE_MAX_MIN", "10"), "DOWNLOAD_RETRY_S": "1", "NO_PROXY": "*", "no_proxy": "*",
               **{k: v for k, v in os.environ.items() if k.startswith("S9_MACHINE_")}}
        args = ["run", "-d", "--name", cname, "--hostname", name, "--network", self.network,
                "--label", f"project={TAG}", "--label", f"step9.type={mtype}", "--label", f"step9.role={role}",
                f"--cpus={min(vcpu, self.vcpu_cap)}", f"--memory={gib}g"]
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
        cid = self._docker(*args, self.image, "bash", "-c", user_data)
        ip = json.loads(self._docker("inspect", cid))[0]["NetworkSettings"]["Networks"][self.network]["IPAddress"]
        return cid[:12], ip

    def alive(self):
        out = self._docker("ps", "-a", "--filter", f"label=project={TAG}", "--format", '{{.ID}} {{.Label "step9.type"}} {{.State}}')
        return [(l.split()[0], l.split()[1]) for l in out.splitlines() if l.strip() and l.split()[2] in ("running", "created", "restarting")]

    def terminate(self, ids):
        for i in ids:
            self._docker("rm", "-f", i, check=False)

    def wait_terminated(self, ids):
        pass

    def leftovers(self, since_iso=None):
        left = []
        out = self._docker("ps", "-a", "--filter", f"label=project={TAG}", "--format", "{{.ID}} {{.Names}} {{.State}}")
        left += [f"container {l}" for l in out.splitlines() if l.strip()]
        out = self._docker("network", "ls", "--filter", f"label=project={TAG}", "--format", "{{.Name}}")
        left += [f"network {l}" for l in out.splitlines() if l.strip()]
        left += [f"bucket {b}" for b in self.buckets()]
        return left

    def cleanup(self, say=print):
        out = self._docker("ps", "-a", "--filter", f"label=project={TAG}", "--format", "{{.ID}}")
        ids = [l for l in out.splitlines() if l.strip()]
        if ids:
            self.terminate(ids)
            say(f"containers removed: {' '.join(ids)}")
        for b in self.buckets():
            self.delete_bucket(b)
            say(f"bucket deleted: {b}")
        for n in self._docker("network", "ls", "--filter", f"label=project={TAG}", "--format", "{{.Name}}").splitlines():
            if n.strip():
                self._docker("network", "rm", n.strip(), check=False)
                say(f"network removed: {n.strip()}")


def make(mode):
    if mode == "aws":
        return AwsPlatform(disk_gb=int(os.environ.get("DISK_GB", "20")))
    if mode == "local":
        return LocalPlatform()
    raise ValueError(mode)
