#!/usr/bin/env bash
# Everything before run_competitors.sh: sources of the earlier sessions, venv, Postgres, mongod, real data.
# Run as root from anywhere. Large files go to $C2 (outside git).
set -euo pipefail
C2=${C2:-/home/user/c2data}
ENG=/home/user/fenvix-engine
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$C2"/{src/owner,src/reviewer,pg,mongo,dumps,logs,data}

# 1. The earlier sessions' code and published results (never checked out in the repo)
git -C $ENG fetch -q origin checkpoint-delta claude/quirky-lamport-p2v27w
git -C $ENG archive origin/checkpoint-delta research/checkpoint-delta | tar -x -C "$C2/src/owner"
git -C $ENG archive origin/claude/quirky-lamport-p2v27w research/checkpoint-delta | tar -x -C "$C2/src/reviewer"
rm -rf "$C2/run" && cp -r "$C2/src/reviewer" "$C2/run"   # working copy: the runs write their results here

# 2. Python venv, versions pinned by the owner
python3 -m venv "$C2/.venv"
"$C2/.venv/bin/pip" install -q langgraph==1.2.20 langgraph-checkpoint==4.3.1 langgraph-checkpoint-postgres==3.2.0 \
  langgraph-checkpoint-mongodb==0.5.1 langgraph-checkpoint-sqlite==3.1.3 langchain==1.4.9 "psycopg[binary]" \
  psycopg-pool pymongo zstandard lz4

# 3. Postgres 16 on 127.0.0.1:5432 (user postgres, password pg, pglz TOAST); runs as the postgres user
chown -R postgres "$C2/pg"; chmod 755 /home/user "$C2"
echo pg > /tmp/pgpw; chmod 644 /tmp/pgpw
su postgres -c "/usr/lib/postgresql/16/bin/initdb -D $C2/pg/data -U postgres --pwfile=/tmp/pgpw -A scram-sha-256"
rm /tmp/pgpw
echo "default_toast_compression = pglz" >> "$C2/pg/data/postgresql.conf"
su postgres -c "/usr/lib/postgresql/16/bin/pg_ctl -D $C2/pg/data -l $C2/pg/pg.log -o '-h 127.0.0.1 -p 5432 -k /tmp' start"

# 4. mongod 7.0.34 from judge/setup.sh, standalone on 27017
~/.fenvix-judge/env/bin/mongod --dbpath "$C2/mongo" --port 27017 --bind_ip 127.0.0.1 \
  --wiredTigerCacheSizeGB 1 --fork --logpath "$C2/logs/mongod.log"

# 5. Real trajectories (reviewer's sources). convert_real.py reads ~/data, so it runs with HOME=$C2.
cd "$C2/data"
git clone -q --depth 1 https://github.com/sierra-research/tau-bench.git tau-bench   # used: 59a200c6
S3=https://swe-bench-submissions.s3.amazonaws.com
list() {  # all keys under a prefix, via the public S3 REST listing
  local tok="" url
  while :; do
    url="$S3/?list-type=2&prefix=$1"
    [ -n "$tok" ] && url="$url&continuation-token=$(python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=""))' "$tok")"
    curl -sS "$url" > page.xml
    python3 -c 'import re;[print(k) for k in re.findall(r"<Key>([^<]+)</Key>",open("page.xml").read())]'
    tok=$(python3 -c 'import re;m=re.search(r"<NextContinuationToken>([^<]+)<",open("page.xml").read());print(m.group(1) if m else "")')
    [ -z "$tok" ] && break
  done
  rm -f page.xml
}
# SWE-agent + Claude 3.5 Sonnet: the reviewer's sample is exactly shuf -n 80 on the sorted key list READ
# FROM A FILE (from a pipe, GNU shuf -n samples differently and gives another set).
R35=20240620_sweagent_claude3.5sonnet
list "lite/$R35/trajs/" | grep '\.traj$' | sort > all_$R35.txt
shuf -n 80 --random-source=<(yes 20261109) all_$R35.txt > sample_$R35.txt
mkdir -p swe/$R35
while read -r k; do curl -sS --retry 5 -o "swe/$R35/$(basename "$k")" "$S3/$k"; done < sample_$R35.txt
diff <(sed 's#.*/##' sample_$R35.txt | sort) "$HERE/swe35_sample.txt"
# SWE-agent + Claude 4 Sonnet: no shuf variant reproduced the reviewer's sample, so the 80 files were
# identified from the reviewer's results/real/swe4.json (per-thread checkpoint count and logical bytes,
# in file order) and are listed in swe4_sample.txt.
R4=20250526_sweagent_claude-4-sonnet-20250514
mkdir -p swe/$R4
while read -r f; do d=${f%.traj}; curl -sS --retry 5 -o "swe/$R4/$f" "$S3/lite/$R4/trajs/$d/$f"; done < "$HERE/swe4_sample.txt"

cd "$C2/run/research/checkpoint-delta/review"
mkdir -p real
HOME=$C2 "$C2/.venv/bin/python" -I convert_real.py real
