# walbench: AWS permissions needed for phase 2

Phase 1 made no AWS call. Phase 2 runs from GitHub Actions (`.github/workflows/walbench.yml`) on branch `walbench`,
in **eu-west-1**, with the existing OIDC role (the repository secret `AWS_ROLE_ARN`, today's role `fenvix-gha-tests`).
Three changes are needed, all made by you in the AWS console or CLI. Replace `<ACCOUNT_ID>` with the account id
(it is not written anywhere in this repository).

Everything walbench creates is named `fenvix-walbench-<run id>...` and tagged `project=fenvix-walbench`; the policies
below are limited to those names / tags wherever AWS supports it.

## 1. OIDC trust: allow branch `walbench`

The role trusts only `round3` today. Add `walbench` to the `sub` condition of the role's trust policy, leaving
`round3` as it is (so the running `round3` session is not affected):

```json
"Condition": {
  "StringEquals": { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
  "StringLike": {
    "token.actions.githubusercontent.com:sub": [
      "repo:Mircoguidetti/fenvix-engine:ref:refs/heads/round3",
      "repo:Mircoguidetti/fenvix-engine:ref:refs/heads/walbench"
    ]
  }
}
```

(If your policy uses `StringEquals` for `sub`, add the second value to that list instead.)

## 2. New role + instance profile for the EC2 instances: `fenvix-walbench-ec2`

The benchmark runs on EC2 (client, 3 quorum nodes, the EBS standby). They get credentials only from this instance
profile; no key is ever passed to them.

Trust policy:

```json
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow", "Principal": { "Service": "ec2.amazonaws.com" }, "Action": "sts:AssumeRole" }] }
```

Inline policy `fenvix-walbench-ec2`:

```json
{ "Version": "2012-10-17",
  "Statement": [
    { "Sid": "S3StandardLogAndRunFiles", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::fenvix-walbench-*", "arn:aws:s3:::fenvix-walbench-*/*"] },
    { "Sid": "S3ExpressLog", "Effect": "Allow",
      "Action": "s3express:CreateSession",
      "Resource": "arn:aws:s3express:eu-west-1:<ACCOUNT_ID>:bucket/fenvix-walbench-*--x-s3" },
    { "Sid": "DynamoDBLog", "Effect": "Allow",
      "Action": ["dynamodb:PutItem", "dynamodb:Query"],
      "Resource": "arn:aws:dynamodb:eu-west-1:<ACCOUNT_ID>:table/fenvix-walbench-*" }
  ] }
```

CLI, once:

```bash
aws iam create-role --role-name fenvix-walbench-ec2 --assume-role-policy-document file://trust-ec2.json
aws iam put-role-policy --role-name fenvix-walbench-ec2 --policy-name fenvix-walbench-ec2 --policy-document file://ec2-policy.json
aws iam create-instance-profile --instance-profile-name fenvix-walbench-ec2
aws iam add-role-to-instance-profile --instance-profile-name fenvix-walbench-ec2 --role-name fenvix-walbench-ec2
```

No SSM, no SSH, no key pair: instances pull their files from the run's S3 bucket and talk to the orchestrator
through small objects in it. The security group opens only TCP 7700 between members of the same group (quorum).

## 3. Inline policy added to the GitHub role (`fenvix-gha-tests`): `fenvix-walbench`

What the orchestrator (`tools/walbench/orchestrate.py`) and the teardown (`tools/walbench/infra.py`) call, nothing more:

```json
{ "Version": "2012-10-17",
  "Statement": [
    { "Sid": "Prices", "Effect": "Allow", "Action": "pricing:GetProducts", "Resource": "*" },

    { "Sid": "S3Buckets", "Effect": "Allow",
      "Action": ["s3:CreateBucket", "s3:DeleteBucket", "s3:PutBucketTagging", "s3:PutLifecycleConfiguration",
                 "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetObject", "s3:PutObject",
                 "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": ["arn:aws:s3:::fenvix-walbench-*", "arn:aws:s3:::fenvix-walbench-*/*"] },
    { "Sid": "S3ListForVerify", "Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*" },

    { "Sid": "S3Express", "Effect": "Allow",
      "Action": ["s3express:CreateBucket", "s3express:DeleteBucket", "s3express:CreateSession", "s3express:TagResource"],
      "Resource": "arn:aws:s3express:eu-west-1:<ACCOUNT_ID>:bucket/fenvix-walbench-*--x-s3" },
    { "Sid": "S3ExpressListForVerify", "Effect": "Allow", "Action": "s3express:ListAllMyDirectoryBuckets", "Resource": "*" },

    { "Sid": "DynamoDB", "Effect": "Allow",
      "Action": ["dynamodb:CreateTable", "dynamodb:DescribeTable", "dynamodb:DeleteTable", "dynamodb:TagResource"],
      "Resource": "arn:aws:dynamodb:eu-west-1:<ACCOUNT_ID>:table/fenvix-walbench-*" },
    { "Sid": "DynamoDBListForVerify", "Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*" },

    { "Sid": "EC2Describe", "Effect": "Allow",
      "Action": ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeImages", "ec2:DescribeInstances",
                 "ec2:DescribeVolumes", "ec2:DescribeSecurityGroups", "ec2:DescribeNetworkInterfaces"],
      "Resource": "*" },

    { "Sid": "RunInstancesTaggedC6iLargeOnly", "Effect": "Allow", "Action": "ec2:RunInstances",
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:volume/*",
                   "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:network-interface/*"],
      "Condition": { "StringEquals": { "aws:RequestTag/project": "fenvix-walbench" } } },
    { "Sid": "RunInstancesInstanceType", "Effect": "Deny", "Action": "ec2:RunInstances",
      "Resource": "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*",
      "Condition": { "StringNotEquals": { "ec2:InstanceType": "c6i.large" } } },
    { "Sid": "RunInstancesUsing", "Effect": "Allow", "Action": "ec2:RunInstances",
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:subnet/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:security-group/*",
                   "arn:aws:ec2:eu-west-1::image/*", "arn:aws:ec2:eu-west-1::snapshot/*"] },

    { "Sid": "CreateTaggedVolumeAndGroup", "Effect": "Allow",
      "Action": ["ec2:CreateVolume", "ec2:CreateSecurityGroup"],
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:volume/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:security-group/*"],
      "Condition": { "StringEquals": { "aws:RequestTag/project": "fenvix-walbench" } } },
    { "Sid": "CreateSecurityGroupInVpc", "Effect": "Allow", "Action": "ec2:CreateSecurityGroup",
      "Resource": "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:vpc/*" },
    { "Sid": "TagOnCreateOnly", "Effect": "Allow", "Action": "ec2:CreateTags", "Resource": "*",
      "Condition": { "StringEquals": { "ec2:CreateAction": ["RunInstances", "CreateVolume", "CreateSecurityGroup"] } } },

    { "Sid": "ActOnTaggedOnly", "Effect": "Allow",
      "Action": ["ec2:TerminateInstances", "ec2:AttachVolume", "ec2:DetachVolume", "ec2:DeleteVolume",
                 "ec2:AuthorizeSecurityGroupIngress", "ec2:DeleteSecurityGroup"],
      "Resource": ["arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:instance/*", "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:volume/*",
                   "arn:aws:ec2:eu-west-1:<ACCOUNT_ID>:security-group/*"],
      "Condition": { "StringEquals": { "aws:ResourceTag/project": "fenvix-walbench" } } },

    { "Sid": "PassOnlyTheWalbenchInstanceRole", "Effect": "Allow", "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/fenvix-walbench-ec2",
      "Condition": { "StringEquals": { "iam:PassedToService": "ec2.amazonaws.com" } } },

    { "Sid": "VerifyNothingLeft", "Effect": "Allow", "Action": "tag:GetResources", "Resource": "*" },

    { "Sid": "ReadVcpuQuota", "Effect": "Allow", "Action": "servicequotas:GetServiceQuota",
      "Resource": "arn:aws:servicequotas:eu-west-1:<ACCOUNT_ID>:ec2/L-1216C47A" }
  ] }
```

Notes on the policy:
- `snapshot/*` (no account id: AWS-owned) is the root-volume snapshot of the Canonical Ubuntu AMI; RunInstances
  authorizes it together with the image.
- `servicequotas:GetServiceQuota` on quota `L-1216C47A` (EC2 "Running On-Demand Standard instances", in vCPUs) is
  read before anything is launched. Rule: at most 3 c6i.large at once; 4 only if the free quota (quota minus the
  vCPUs of every pending/running instance in eu-west-1, from DescribeInstances) is at least 8. If the quota cannot be
  read (e.g. AccessDenied) the run uses 3. With 3, quorum node 0 runs as a process on the client instance (same AZ
  as node 0 would be; the report says so); with fewer than 6 free vCPUs nothing is launched. The free quota is logged.
- `ec2:AuthorizeSecurityGroupIngress` is allowed only on the tagged walbench security group (the rule itself is not
  tagged; it is deleted with its group).
- The `Deny` limits the role to `c6i.large` for anything it launches; the `RequestTag` condition makes every instance,
  volume and network interface carry `project=fenvix-walbench` from creation, and the delete/terminate/attach actions
  only work on resources with that tag.
- `s3express:CreateSession` is needed by the teardown (listing and deleting the directory bucket's objects).
  `s3express:TagResource` is used to tag the directory bucket; if AWS refuses it, the run continues (the bucket is
  found and deleted by its name prefix) and the result says the bucket was not tagged.
- No IAM write action is given to the GitHub role: it only passes the existing `fenvix-walbench-ec2` role.

## Account prerequisites (please check, no change expected)

- A **default VPC in eu-west-1** with default subnets in at least 3 AZs (the benchmark uses it; it creates no VPC).
- On-demand **vCPU quota** for standard instances in eu-west-1: at least 6 vCPUs free (3 × c6i.large); 8 free lets
  quorum node 0 run on its own instance (see the quota rule above).
- S3 Express One Zone available in one of the AZ ids `euw1-az1`, `euw1-az3`, `euw1-az2` (tried in this order).

## How phase 2 starts (after you confirm)

I commit `tools/walbench/go.json` on branch `walbench`; that push is the only trigger of the run. `round3` and its
workflow are not touched. A push of `tools/walbench/teardown.json` runs only the teardown + verification, as an
emergency clean-up.
