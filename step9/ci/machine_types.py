"""Step 9 · the machine types the round may use, with vCPU, memory, CPU family and the on-demand Linux price in
eu-west-1 (dollars per hour).

Prices DECLARED: official AWS Price List, eu-west-1 EC2 offer file published 2026-11-04T21:03Z
(https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.json), copied in fenvix-brain
sessions/2026-11-05-inventario-cucina-minima/prezzi/prezzi-ec2-eu-west-1.txt. c7i.2xlarge/4xlarge/8xlarge are 2/4/8
times c7i.xlarge (c7i.2xlarge 0.383 DECLARED in kitchen/ci/machine_types.py).
EBS gp3 $0.088 per GB-month and public IPv4 $0.005 per hour: DECLARED (AWS price list, same file / VPC pricing page).

Atlas M30 nodes are 2 vCPU and 8 GB (DECLARED, Atlas console 2026-11-05 "M30 Gen2 from $0.77/hr, 8 GB, 2 vCPU"), so the
matching types for the cook and the warm spare are:
  x86_64:   m7i.large  (2 vCPU, 8 GiB) $0.1124/h
  Graviton: m7g.large  (2 vCPU, 8 GiB) $0.0910/h, m8g.large (2 vCPU, 8 GiB) $0.1000/h

Usage (prints on stdout; exit 1 = refused, with the reason):
  machine_types.py check ROLE TYPE           -> "TYPE VCPU ARCH PRICE" if TYPE is allowed for ROLE
  machine_types.py arch TYPE                 -> x86_64 or aarch64
  machine_types.py vcpu TYPE
  machine_types.py minute-cost DISK_GB TYPE... -> dollars of one minute for these machines (machine, gp3 disk, IPv4)
"""
import sys

# type: (vCPU, GiB, CPU, dollars per hour)
TYPES = {
    "m7i.large": (2, 8, "x86_64", 0.1124),
    "m7g.large": (2, 8, "aarch64", 0.0910),
    "m8g.large": (2, 8, "aarch64", 0.1000),
    "m7g.medium": (1, 4, "aarch64", 0.0455),
    "c7g.medium": (1, 2, "aarch64", 0.0388),
    "c7g.large": (2, 4, "aarch64", 0.0775),
    "c7i.large": (2, 4, "x86_64", 0.0958),
    "c7i.xlarge": (4, 8, "x86_64", 0.1915),
    "c7i.2xlarge": (8, 16, "x86_64", 0.383),
    "c7i.4xlarge": (16, 32, "x86_64", 0.766),
    "c7i.8xlarge": (32, 64, "x86_64", 1.532),
}
# which types each role may use (a command naming another type is refused)
ROLES = {
    "cook": ["m7i.large", "m7g.large", "m8g.large"],          # matches an M30 node (2 vCPU, 8 GB)
    "spare": ["m7i.large", "m7g.large", "m8g.large"],         # same as the cook: the fair-comparison rule
    "log": ["m7g.medium", "c7g.medium", "m7g.large", "m7i.large"],
    "compactor": ["c7g.large", "m7g.large", "m8g.large", "m7i.large"],
    "loader": ["m7i.large", "c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge"],  # cook 0: loads, never measured
    "client": ["c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge", "c7i.8xlarge"],  # x86_64 (the founder's script)
    "extras_client": ["c7i.large", "c7i.xlarge"],
    "pgbox": ["m7i.large", "m7g.large"],                       # the Postgres machine of the agent benchmark
    "filler": ["c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge", "c7i.8xlarge"],  # loads the ballast for 100 GB / 1 TB
    "generator": ["c7i.xlarge", "c7i.2xlarge"],               # generates the ballast in our format (alone: 8 vCPU)
    "mongod": ["m7i.large"],                                   # the MongoDB line: 3 members like an M30's (x86 binary)
}
EBS_GB_MONTH = 0.088
IPV4_HOUR = 0.005
HOURS_PER_MONTH = 730


def no(msg):
    print(msg)
    sys.exit(1)


def get(t):
    if t not in TYPES:
        no(f"type not known: {t} (known: {', '.join(TYPES)})")
    return TYPES[t]


def minute_cost(disk_gb, types):
    total = 0.0
    for t in types:
        _, _, _, price = get(t)
        total += price + disk_gb * EBS_GB_MONTH / HOURS_PER_MONTH + IPV4_HOUR
    return total / 60


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "check":
        role, t = sys.argv[2], sys.argv[3]
        if role not in ROLES:
            no(f"role not known: {role}")
        if t not in ROLES[role]:
            no(f"{role}: type {t} not allowed (allowed: {', '.join(ROLES[role])})")
        v, _, a, p = get(t)
        print(t, v, a, p)
    elif cmd == "arch":
        print(get(sys.argv[2])[2])
    elif cmd == "vcpu":
        print(get(sys.argv[2])[0])
    elif cmd == "minute-cost":
        print(f"{minute_cost(float(sys.argv[2]), sys.argv[3:]):.8f}")
    else:
        no(__doc__)


if __name__ == "__main__":
    main()
