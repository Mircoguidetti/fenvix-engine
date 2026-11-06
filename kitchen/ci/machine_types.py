"""Kitchen (K2, corrected by K2b) · the allowed machine types, with vCPU, CPU (x86_64 or aarch64) and on-demand Linux
price in eu-west-1 in dollars per hour. Prices DECLARED: official AWS price list (AWS Price List, eu-west-1 file of
2026-11-04, copy in fenvix-brain sessions/2026-11-05-inventario-cucina-minima/prezzi/); c7i.2xlarge, c7i.4xlarge and
c7i.8xlarge are 2, 4 and 8 times c7i.xlarge (c7i.2xlarge 0.383 DECLARED, reviewer of plan B).
t4g.small: in "unlimited" mode (the default of t4g) AWS charges the CPU credits above the baseline at $0.04 per
vCPU-hour for Linux (DECLARED: aws.amazon.com/ec2/pricing/on-demand, "T2/T3/T4g Unlimited Mode Pricing", read by K2b on
2026-11-05: "For T4g instances in Unlimited mode, CPU Credits are charged at $0.04 per vCPU-Hour for Linux, RHEL and
SLES"). Counted here as if the machine always used both vCPUs: 2 x 0.04 = $0.08 per hour more (upper bound).

Usage (prints on stdout; exit 1 = refused, with the reason):
  machine_types.py check-sizes '["m7i.large"]' [MAX]        -> one size per line, if all allowed, no duplicates, at most MAX
  machine_types.py arch TYPE                                 -> x86_64 or aarch64
  machine_types.py vcpu TYPE
  machine_types.py archs '["m7i.large","c7g.large"]' [LOADER] -> the CPUs to build for, space separated
  machine_types.py client QUOTA SIZES LOADER CLIENT          -> "CLIENT NEEDED" if CLIENT plus the biggest cook fits the quota
  machine_types.py minute-cost DISK_GB TYPE...               -> dollars of one minute for these machines (machine, gp3, IPv4)
"""
import json
import sys

# type: (vCPU, CPU, dollars per hour)
SERVER = {
    "c7i.large": (2, "x86_64", 0.0958),
    "c7i.xlarge": (4, "x86_64", 0.1915),
    "m7i.large": (2, "x86_64", 0.1124),
    "c7g.medium": (1, "aarch64", 0.0388),
    "c7g.large": (2, "aarch64", 0.0775),
    "m7g.large": (2, "aarch64", 0.0910),
    "t4g.small": (2, "aarch64", 0.0184 + 2 * 0.04),  # plus the "unlimited" CPU credits, upper bound
}
CLIENT = {  # the machine that sends the load (the script), always x86_64
    "c7i.xlarge": (4, "x86_64", 0.1915),
    "c7i.2xlarge": (8, "x86_64", 0.383),
    "c7i.4xlarge": (16, "x86_64", 0.766),
    "c7i.8xlarge": (32, "x86_64", 1.532),
}
ALL = {**SERVER, **CLIENT}
EBS_GB_MONTH = 0.088  # gp3, DECLARED (AWS price list eu-west-1)
IPV4_HOUR = 0.005     # public IPv4, DECLARED (AWS)


def no(msg):
    print(msg)
    sys.exit(1)


def sizes(js, most=4, empty_ok=False):
    try:
        t = json.loads(js)
    except Exception:
        no(f"sizes not readable: {js!r} (example: [\"m7i.large\"])")
    if isinstance(t, str):
        t = [t]
    if not isinstance(t, list) or not all(isinstance(x, str) for x in t) or (not t and not empty_ok):
        no("sizes: a list of types is needed, for example [\"m7i.large\"]")
    out = [x for x in t if x not in SERVER]
    if out:
        no(f"sizes not allowed: {', '.join(out)} (allowed: {', '.join(SERVER)})")
    if len(set(t)) != len(t):
        no("sizes: the same size twice")
    if len(t) > most:
        no(f"sizes: at most {most} per run (a GitHub job lasts at most 6 hours)")
    return t


def main():
    c = sys.argv[1]
    if c == "check-sizes":
        most = int(sys.argv[3]) if len(sys.argv) > 3 else 4
        print("\n".join(sizes(sys.argv[2], most, empty_ok=True)))
    elif c == "arch":
        print(ALL[sys.argv[2]][1])
    elif c == "vcpu":
        print(ALL[sys.argv[2]][0])
    elif c == "archs":
        t = sizes(sys.argv[2], 99, empty_ok=True) + ([sys.argv[3]] if len(sys.argv) > 3 and sys.argv[3] else [])
        print(" ".join(sorted({SERVER[x][1] for x in t if x in SERVER})))
    elif c == "client":
        quota, t, loader, cl = float(sys.argv[2]), sizes(sys.argv[3], 99, empty_ok=True), sys.argv[4], sys.argv[5]
        if cl not in CLIENT:
            no(f"client {cl} not allowed (allowed: {', '.join(CLIENT)})")
        if loader and loader not in SERVER:
            no(f"loader {loader} not allowed (allowed: {', '.join(SERVER)})")
        cooks = t + ([loader] if loader else [])
        vmax = max((SERVER[x][0] for x in cooks), default=0)
        needed = CLIENT[cl][0] + vmax
        if needed > quota:
            no(f"vCPU quota {quota:g}: {cl} ({CLIENT[cl][0]} vCPU) plus the biggest cook ({vmax} vCPU) needs {needed}")
        print(f"{cl} {needed}")
    elif c == "minute-cost":
        gb = float(sys.argv[2])
        tot = 0.0
        for x in sys.argv[3:]:
            p = ALL.get(x, (0, "", max(v[2] for v in ALL.values())))[2]  # unknown type: the highest price
            tot += (p + gb * EBS_GB_MONTH / 730 + IPV4_HOUR) / 60
        print(f"{tot:.6f}")
    else:
        no(f"unknown command: {c}")


if __name__ == "__main__":
    main()
