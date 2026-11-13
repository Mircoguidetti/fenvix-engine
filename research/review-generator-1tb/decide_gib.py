import sys
sys.path.insert(0, "/home/user/eng-gen/step9/local/tests")
import test_generator_1tb as t
for gib in [931.4, 130, 931.3225746, 100.123456789]:
    rc, out, r = t.decide(data="1tb", pantry="keep", ballast_gib=gib)
    if r is None:
        print(gib, "rc", rc, out[-300:]); continue
    l = r["ballast"]["lines"][0]
    print(gib, "rc", rc, "command ballast_gib", r["ballast_gib"], "plan line --gb", l[l.index("--gb") + 1], "jobs", {k: r[k] for k in r if "job" in k or "hours" in k})
