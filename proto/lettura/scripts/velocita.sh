#!/usr/bin/env bash
# Passo 2: un database a 20.000 scritture/s da 1,2 KB (~24 MB/s). Lo scarico (pl-worker) gira su 2 core (taskset
# -c 2,3); registro con il generatore e MinIO sugli altri 2 (0,1). Uso: velocita.sh durata_s uscita [opzioni worker]
set -u
DUR=${1:-300}; OUT=${2:-/tmp/claude-0/run/velocita}; shift 2 || true
B=$(dirname "$0")/../target/release
PY=${PY:-/tmp/claude-0/venv/bin/python}
mkdir -p "$OUT"
$PY "$(dirname "$0")/bucket.py" --svuota velocita > /dev/null
MPID=$(pgrep -f "minio server" | head -1)
taskset -a -p -c 0,1 "$MPID" > /dev/null
taskset -c 0,1 "$B/pl-registro" --addr 127.0.0.1:7730 --db 1 --rate 20000 --scritture 1 --val-min 1200 --val-max 1200 \
  --chiavi 10000000 --stat-ms 5000 > "$OUT/registro.jsonl" 2> "$OUT/registro.err" &
RP=$!
sleep 1
taskset -c 2,3 "$B/pl-worker" --addr 127.0.0.1:7730 --bucket velocita --eta-ms 1000 --dim $((8<<20)) \
  --max-batch $((16<<20)) --no-compatta --stat-ms 5000 --intervallo-ms 20 "$@" > "$OUT/worker.jsonl" 2> "$OUT/worker.err" &
WP=$!
sleep "$DUR"
kill $WP $RP
taskset -a -p -c 0-3 "$MPID" > /dev/null
python3 - "$OUT" <<'P'
import json, sys
o = sys.argv[1]
r = [json.loads(l) for l in open(o + "/registro.jsonl")]
w = [json.loads(l) for l in open(o + "/worker.jsonl")]
# si scarta il primo minuto (avvio)
w2 = [x for x in w if x["t_s"] >= 60]; r2 = [x for x in r if x["t_s"] >= 60]
a, z = w2[0], w2[-1]
dt = z["t_s"] - a["t_s"]
ra, rz = r2[0], r2[-1]
res = {
  "secondi_misurati": dt,
  "commit_s": (rz["reg"]["commit"] - ra["reg"]["commit"]) / (rz["t_s"] - ra["t_s"]),
  "record_scaricati_s": (z["stat"]["record"] - a["stat"]["record"]) / dt,
  "mb_sst_s": (z["stat"]["byte_sst"] - a["stat"]["byte_sst"]) / dt / 1e6,
  "scarichi_s": (z["stat"]["scarichi"] - a["stat"]["scarichi"]) / dt,
  "core_worker": (z["cpu_s"] - a["cpu_s"]) / dt,
  "core_registro_e_generatore": (rz["cpu_s"] - ra["cpu_s"]) / (rz["t_s"] - ra["t_s"]),
  "ritardo_ms_max": max(x["reg"]["ritardo_ms"] for x in r2),
  "ritardo_ms_ultimo": rz["reg"]["ritardo_ms"],
  "non_su_s3_mb_max": max(x["reg"]["pend_byte"] for x in r2) / 1e6,
  "rss_worker_mb_max": max(x["rss"] for x in w2) / 1e6,
  "errori_worker": z["stat"]["errori"],
}
json.dump(res, open(o + "/riassunto.json", "w"), indent=1)
print(json.dumps(res, indent=1))
P
