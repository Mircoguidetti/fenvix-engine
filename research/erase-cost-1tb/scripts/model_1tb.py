"""erase-cost-1tb: rewrite bytes per day at 1 TB, before (perf-erase hourly batch scrub) and after (ride + deadline),
scaled from the numbers measured on the 10 GiB ballast. Every input is printed with its label. Usage: model_1tb.py"""
import json, os

R = os.path.join(os.path.dirname(__file__), "..", "results")
GB = 1e9

# ---- MEASURED on the 10 GiB ballast (results/*.json, manifest of version 1) ----------------------------------------
stored = 8.0e9                    # stored bytes of the database (MinIO du, our runs)
groups = 20                       # groups of version 1
shared_groups = 5                 # loc/ (3) and nat/ (2) groups: locators by _id and natural-order entries
shared_bytes = (432 + 419 + 420 + 407 + 187) * 1e6   # their sealed runs
group_bytes = 444e6               # one customer group (u/ space), stored
b1 = json.load(open(os.path.join(R, "base-1cust.json")))["run"]["minio"]
b10 = json.load(open(os.path.join(R, "base-10cust.json")))["run"]["minio"]
r1 = json.load(open(os.path.join(R, "ride-1cust.json")))["run"]["minio"]
r10 = json.load(open(os.path.join(R, "ride-10cust.json")))["run"]["minio"]

def puts(m):
    return sum(v for k, v in m.items() if k in ("req.putobject", "req.putobjectpart", "req.newmultipartupload", "req.completemultipartupload"))

print("MEASURED (10 GiB ballast, 8.0 GB stored, 20 groups, 5 of them shared loc/nat = %.2f GB, %.0f%% of stored)" % (shared_bytes / GB, 100 * shared_bytes / stored))
for name, m in (("before, 1 customer, scrub", b1), ("before, 10 customers, scrub (one abandon: 2 scrubs)", b10), ("after, 1 customer, forced", r1), ("after, 10 customers, forced", r10)):
    print(f"  {name}: written {m['received_bytes'] / GB:.2f} GB, read {m['sent_bytes'] / GB:.2f} GB, PUT-class {puts(m):.0f}, GET {m.get('req.getobject', 0):.0f}")
# what one rewrite touches per erasure, MEASURED: all shared groups + the customer's own group (+ small delta runs)
per_batch_fixed = shared_bytes                     # every batch, whatever its size
per_customer = group_bytes                         # one more group per customer (distinct groups)

# ---- ASSUMPTIONS for 1 TB (ESTIMATED) -------------------------------------------------------------------------------
size = 1e12
customers = 1_000_000
erased_per_month = 0.01 * customers                # 1% of customers a month (the perf-erase owner's assumption)
per_hour = erased_per_month / 30 / 24
per_day = per_hour * 24
shared = size * shared_bytes / stored              # same layout share as the ballast
n_groups = size / group_bytes
n_customer_groups = n_groups * (groups - shared_groups) / groups
work_h = 233e9 / 20e6 / 3600                         # the adaptive force time: owing shared bytes at 20 MB/s (MEASURED reseal speed)
force_cycle_h = 18.0 - 1.0 - work_h                 # shared groups force-resealed once per cycle: 18 h force time, minus the 1 h window, minus the work estimate
print("\nASSUMED for 1 TB: %.0f customers, %.1f%% erased a month = %.1f an hour, %.0f a day; layout as the ballast: shared %.0f GB, %.0f groups of %.2f GB (%.0f customer groups)"
      % (customers, 100 * erased_per_month / customers, per_hour, per_day, shared / GB, n_groups, group_bytes / GB, n_customer_groups))

def distinct(k, n):
    """Expected distinct groups hit by k customers spread uniformly over n groups."""
    return n * (1 - (1 - 1 / n) ** k)

# before: an hourly batch rewrites every shared group and the distinct groups of its customers
before_batch = shared + distinct(per_hour, n_customer_groups) * group_bytes
before_day = 24 * before_batch
# after, no ordinary reseal helping (worst case: every group cold): shared groups once per force cycle, every erased
# customer's group once (forced in the pass of its window: the customers of one hour share it)
after_day = shared * 24 / force_cycle_h + 24 * distinct(per_hour, n_customer_groups) * group_bytes
print("\nESTIMATED rewrite bytes a day at 1 TB (delta runs not counted on either side):")
print(f"  before (hourly batch scrub): {before_day / GB:,.0f} GB a day ({before_day / per_day / GB:.1f} GB per erased customer)")
print(f"  after, no ordinary reseal helps (worst case): {after_day / GB:,.0f} GB a day ({after_day / per_day / GB:.2f} GB per erased customer), {before_day / after_day:.1f} times less")
for f in (0.5, 0.9):
    a = shared * 24 / force_cycle_h + (1 - f) * 24 * distinct(per_hour, n_customer_groups) * group_bytes
    print(f"  after, {int(f * 100)}% of the erased customers' groups resealed by ordinary passes within the force cycle: {a / GB:,.0f} GB a day")
# the floor of this design: the shared groups, once a day at best
print(f"  floor of this design (shared groups once per cycle, nothing else): {shared * 24 / force_cycle_h / GB:,.0f} GB a day")
# time: a reseal of one group, MEASURED (forced, 1 customer: 6 reseals in the pass)
t_group = json.load(open(os.path.join(R, "ride-10cust.json")))["run"]["s"] / 13   # 10 reseals + 3 cleanses
forced_s = (shared / group_bytes + distinct(per_hour, n_customer_groups)) * t_group
print(f"\nESTIMATED longest forced pass at 1 TB (every shared group and one hour of customers' groups), one reseal at a time ({t_group:.0f} s per group, MEASURED here): {forced_s / 3600:.1f} h")
print(f"  with the adaptive force time the pass starts {work_h:.1f} h (+ the 1 h window) before 18 h; physically gone ESTIMATED within about {force_cycle_h + forced_s / 3600 + 0.5:.1f} h of the request (+0.5 h: GC grace 10 min and the SlateDB drop, about 17 min MEASURED by the perf-bytes reviewer)")
print(f"ESTIMATED one hourly batch scrub before, at 1 TB: {(shared / group_bytes + distinct(per_hour, n_customer_groups)) * t_group / 3600:.1f} h of rewriting per hour of erasures")
