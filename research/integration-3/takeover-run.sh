#!/bin/bash
# integration-3: the step 9 harness (branch aws-step9, local-run.sh's round function unchanged) running ONLY the
# takeovers program, one round per log mode. Usage: takeover-run.sh S9WT WORK LOGMODE(on|off) N
set -u
# integration-3: the binaries under test are this branch's (built in /home/user/fenvix-engine)
export SRV=${SRV:-/home/user/fenvix-engine/target/release/mongo-server} PQ=${PQ:-/home/user/fenvix-engine/proto/quaderno/target/release/pq}
S9WT=$1; W=$2; LOGMODE=$3; N=$4
cd "$S9WT/step9/local" || exit 1
# take local-run.sh up to (not including) its job list: setup, MinIO, fake Atlas, binaries, round()
sed -n '1,/^D=\$W\/commands; mkdir -p "\$D"$/p' local-run.sh | sed "s#^HERE=.*#HERE=$S9WT/step9/local; S9=$S9WT/step9; ENG=$S9WT#" > "$W.head.sh"
python3 - "$W.head.sh" <<'PYX'
import sys
p=sys.argv[1]; s=open(p).read()
old='docker build -q -t fenvix-step9-machine "$HERE" > /dev/null || exit 1'
assert old in s
s=s.replace(old, 'docker image inspect fenvix-step9-machine > /dev/null 2>&1 || '+old)
s=s.replace('(cd "$ENG" && git rev-parse --short HEAD)', '(cd /home/user/fenvix-engine && git rev-parse --short HEAD)')
open(p,"w").write(s)
PYX
{
  cat "$W.head.sh"
  cat <<INNER
echo '{"mode": "run", "data": "atlas", "log": "m7i.large", "phases": ["load", "measure", "extras"], "pantry": "new", "programs": ["takeovers"], "takeovers_args": "--n 50 --writers 8 --between-s 3 --lease-ttl 4 --log $LOGMODE", "ceiling_min": 150}' > "\$D/tk.json"
round takeovers-$LOGMODE "\$D/tk.json"
kill \$MINIO_PID 2>/dev/null
echo "total time: \$(( \$(date +%s) - T0 )) s"
INNER
} > "$W.sh"
TAKEOVERS_N=$N bash "$W.sh" "$W"
