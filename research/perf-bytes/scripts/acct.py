"""Print a byte accounting from fx_bytes JSON (and the run's summary.json for logical bytes)."""
import json
import sys

d = json.load(open(sys.argv[1]))
s = json.load(open(sys.argv[2])) if len(sys.argv) > 2 else None
logical = s["bytes"]["logical_total"] if s else None
G = 1e9


def g(x):
    return f"{x / G:.3f} GB" + (f" ({x / logical:.4f} per logical byte)" if logical else "")


print("total", g(d["total"]))
for k, v in sorted(d["by_kind"].items(), key=lambda kv: -kv[1]["bytes"]):
    print(f"  {k}: {v['objects']} objects, {g(v['bytes'])}")
sl = d.get("slate", {})
print("SlateDB compacted listed in its manifest:", g(sl.get("compacted_listed_bytes", 0)), "; not listed (garbage not deleted yet):", g(sl.get("compacted_unlisted_bytes", 0)))
print("  l0", sl.get("l0"), "sorted runs", sl.get("sorted_runs"), "checkpoints", len(sl.get("checkpoints", [])))
f = d["fx"]
print("fx live runs:", f["live_runs"]["objects"], g(f["live_runs"]["bytes"]), "sealed", f["live_sealed"], "delta", f["live_delta"])
print("fx runs kept only by older retained versions:", f["retained_runs_older_versions_only"]["objects"], g(f["retained_runs_older_versions_only"]["bytes"]))
print("fx runs waiting for the GC grace:", f["unreferenced_runs_grace"]["objects"], g(f["unreferenced_runs_grace"]["bytes"]))
print("fx manifests", g(f["manifests"]), "versions", f["versions"], "oldest version age s", f["oldest_version_age_s"], "groups", f["groups"])
codecs = {}
for r in f["live_run_list"]:
    k = (r["kind"], r["codec"], r["derived"])
    c = codecs.setdefault(k, [0, 0, 0, 0])
    c[0] += 1; c[1] += r["size"]; c[2] += r["raw"]; c[3] += r["derived_rows"]
print("live runs by kind/codec/derived: (n, stored, raw, derived rows)", codecs)
tot_raw = sum(a["key_bytes"] + a["value_bytes"] for a in f["live_classes"].values())
stored_live = f["live_runs"]["bytes"]
print("live classes (raw key+value bytes; stored share estimated in proportion to raw):")
for k, a in sorted(f["live_classes"].items(), key=lambda kv: -(kv[1]["key_bytes"] + kv[1]["value_bytes"])):
    raw = a["key_bytes"] + a["value_bytes"]
    if raw < 50_000:
        continue
    env = {e: (x["rows"], x["bytes"]) for e, x in a["by_envelope"].items()}
    print(f"  {k}: rows {a['rows']}, raw {raw / 1e6:.1f} MB, stored about {raw / max(tot_raw, 1) * stored_live / 1e6:.1f} MB, by envelope {env}")
sr = d.get("slate_rows")
if sr:
    def tot(c):
        return sum(a["key_bytes"] + a["value_bytes"] for a in c.values()), sum(a["rows"] for a in c.values())
    print("SlateDB rows at or below X_d (also in our runs):", tot(sr["at_or_below_x_d"]), "above:", tot(sr["above_x_d"]))
    print("  compression of rows at/below in 4 KiB blocks:", sr["compress_at_or_below"], "above:", sr["compress_above"])
    for k, a in sorted(sr["at_or_below_x_d"].items(), key=lambda kv: -(kv[1]["key_bytes"] + kv[1]["value_bytes"]))[:6]:
        print(f"    below {k}: rows {a['rows']}, raw {(a['key_bytes'] + a['value_bytes']) / 1e6:.1f} MB")
