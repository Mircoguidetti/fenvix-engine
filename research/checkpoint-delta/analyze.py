"""Aggregate every result into results/summary.json and results/summary.txt.

Mix (ESTIMATED): share of threads by length 10: 40%, 40: 35%, 100: 20%, 300: 5%.
Money: 100,000 new threads a month, each kept one year. Month 12 holds 1.2 million threads;
the first year pays 78 x 100,000 thread-months (1 + 2 + ... + 12).
Prices DECLARED in sessions/2026-11-08-chat-fact-check/REPORT.md: Aurora $0.10, Supabase
$0.125, Atlas Infinite $0.30 per GB-month (Atlas bills uncompressed data, so it gets the
Mongo saver's dataSize plus its indexes), S3 $0.023; our price = cost / 0.55.
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(HERE, "results")
LENGTHS = (10, 40, 100, 300)
MIX = {10: 0.40, 40: 0.35, 100: 0.20, 300: 0.05}
THREADS_PER_MONTH = 100_000
GB = 1e9
PRICES = {"aurora": 0.10, "supabase": 0.125, "atlas": 0.30, "s3_cost": 0.023}
PRICES["ours_price"] = PRICES["s3_cost"] / 0.55


def load(p):
    return json.load(open(p)) if os.path.exists(p) else None


def per_thread():
    out = {}
    for n in LENGTHS:
        row = {}
        for mode in ("default", "delta"):
            d = load(os.path.join(R, "savers", f"ck_{mode}_{n}", "disk.json"))
            if d:
                t = d["threads"]
                row[f"pg_{mode}"] = d["postgres"]["total"] / t
                row[f"pg_{mode}_blobs"] = d["postgres"]["checkpoint_blobs"]["total"] / t
                row[f"pg_{mode}_writes"] = d["postgres"]["checkpoint_writes"]["total"] / t
                row[f"pg_{mode}_checkpoints"] = d["postgres"]["checkpoints"]["total"] / t
                row[f"mongo_{mode}_disk"] = d["mongo"]["total_on_disk"] / t
                row[f"mongo_{mode}_data"] = (d["mongo"]["total_data_size"] + sum(
                    v["totalIndexSize"] for v in d["mongo"].values() if isinstance(v, dict))) / t
                row[f"sqlite_{mode}"] = d["sqlite"]["file_bytes"] / t
        d = load(os.path.join(R, "savers", f"ck_default_{n}_lz4", "disk.json"))
        if d:
            row["pg_default_lz4"] = d["postgres"]["total"] / d["threads"]
        o = load(os.path.join(R, "options", f"{n}.json"))
        if o:
            for k, v in o["bytes_per_thread"].items():
                row["ours_" + k] = v
        c = load(os.path.join(R, "options", f"{n}-log-codec.json"))
        if c:
            row["ours_log_lz4"] = c["bytes_per_thread"]["log_lz4"]
        out[n] = row
    return out


def main():
    pt = per_thread()
    keys = sorted(set().union(*[set(v) for v in pt.values()]))
    mix = {k: sum(MIX[n] * pt[n][k] for n in LENGTHS) for k in keys if all(k in pt[n] for n in LENGTHS)}
    stored_threads = 12 * THREADS_PER_MONTH
    thread_months = 78 * THREADS_PER_MONTH
    money = {}
    plans = [
        ("Postgres saver default, Aurora", "pg_default", "aurora"),
        ("Postgres saver default, Supabase", "pg_default", "supabase"),
        ("Postgres saver DeltaChannel, Aurora", "pg_delta", "aurora"),
        ("Postgres saver DeltaChannel, Supabase", "pg_delta", "supabase"),
        ("Mongo saver default, Atlas Infinite (uncompressed)", "mongo_default_data", "atlas"),
        ("Mongo saver DeltaChannel, Atlas Infinite (uncompressed)", "mongo_delta_data", "atlas"),
        ("Ours, full checkpoints zstd, price", "ours_full", "ours_price"),
        ("Ours, thread message log, cost", "ours_log", "s3_cost"),
        ("Ours, thread message log, price", "ours_log", "ours_price"),
        ("Ours, thread message log with lz4 (today's run codec), price", "ours_log_lz4", "ours_price"),
        ("Ours, JSON delta K=10, price", "ours_json_10", "ours_price"),
        ("Ours, binary delta K=10, price", "ours_bin_10", "ours_price"),
    ]
    for label, k, p in plans:
        if k not in mix:
            continue
        gb12 = mix[k] * stored_threads / GB
        money[label] = {
            "bytes_per_thread_mix": mix[k],
            "stored_gb_month12": gb12,
            "usd_month12": gb12 * PRICES[p],
            "usd_first_year": mix[k] * thread_months / GB * PRICES[p],
            "price_per_gb_month": PRICES[p],
        }
    reads = {n: load(os.path.join(R, "options", f"{n}.json")) for n in LENGTHS}
    sreads = {n: load(os.path.join(R, "saver-reads", f"{n}.json")) for n in LENGTHS}
    summary = {"mix": MIX, "per_thread": pt, "mix_per_thread": mix, "money": money, "prices": PRICES,
               "option_reads": {n: (r or {}).get("read") for n, r in reads.items()},
               "rebuild_mismatches": {n: (r or {}).get("rebuild_mismatches") for n, r in reads.items()},
               "saver_reads": sreads}
    json.dump(summary, open(os.path.join(R, "summary.json"), "w"), indent=1)

    L = []
    L.append("Bytes per thread (B), by thread length")
    for k in keys:
        L.append(f"  {k:28s} " + "  ".join(f"{n}:{pt[n].get(k, float('nan')):>13,.0f}" for n in LENGTHS)
                 + (f"   mix:{mix[k]:>12,.0f}" if k in mix else ""))
    L.append("")
    L.append("Ratios on the mix (stored bytes, lower is better)")
    for a, b in [("pg_default", "ours_log"), ("pg_delta", "ours_log"), ("pg_default", "ours_full"),
                 ("pg_delta", "ours_full"), ("pg_default", "ours_json_10"), ("pg_delta", "ours_json_10"),
                 ("pg_default", "ours_bin_10"), ("pg_delta", "ours_bin_10"), ("ours_logical", "ours_log"),
                 ("mongo_default_disk", "ours_log"), ("mongo_delta_disk", "ours_log"), ("pg_default", "pg_delta"), ("pg_default", "ours_log_lz4"), ("pg_delta", "ours_log_lz4"),
                 ("pg_default_lz4", "ours_log"), ("mongo_delta_data", "ours_log"), ("ours_full", "ours_log")]:
        if a in mix and b in mix:
            L.append(f"  {a} / {b} = {mix[a] / mix[b]:.2f}")
    L.append("")
    L.append("Money, 100,000 threads a month kept one year")
    for label, m in money.items():
        L.append(f"  {label:58s} {m['bytes_per_thread_mix']/1e6:8.3f} MB/thread  {m['stored_gb_month12']:10,.0f} GB at month 12"
                 f"  ${m['usd_month12']:10,.0f}/month at month 12  ${m['usd_first_year']:11,.0f} first year  (${m['price_per_gb_month']:.4f}/GB-month)")
    open(os.path.join(R, "summary.txt"), "w").write("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
