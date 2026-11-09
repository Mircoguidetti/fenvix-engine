#!/bin/bash
# Step 9 · the Postgres machine of the agent benchmark: plain PostgreSQL 16 (Ubuntu 24.04's package; the major
# version Supabase runs, DECLARED by Supabase's documentation, to check) on one EC2 machine with its data on the gp3 root
# disk: what a Supabase project is (one Postgres on one machine with EBS), without a Supabase account (founder,
# 2026-11-09). Priced with Supabase's declared prices in the report, not with ours.
# Flow: install postgresql-16 -> listen on the private address, password login for app_user (the round password, from
# the run bucket, never in a file) -> database app_demo -> "ready" -> on "stop": stop Postgres, sizes, power off.
# Usage: pgbox.sh BUCKET
set -u
cd "$(dirname "$0")" || exit 1
BUCKET=$1; P=pgbox
source ./common.sh
finish() {
  step "finish: $1"; [ -n "${CPU_PID:-}" ] && kill "$CPU_PID" 2>/dev/null
  du -sb /var/lib/postgresql > out/pgdata-bytes.txt 2>/dev/null
  service postgresql stop >> out/steps.log 2>&1 || true
  printf '{"finished": true, "outcome": "%s"}\n' "$1" > out/stopped.json
  push; push; $PY s3io.py put "$BUCKET" "$P/stopped" out/stopped.json 2>/dev/null
  power_off "pgbox stopped"
}
IP=$(private_ip); TYPE=$(machine_type); AZ=$(zone)
step "pgbox: $TYPE in $AZ, private IP $IP, $(nproc) vCPU"
$PY cpu_macchina.py out/cpu-machine.log 5 & CPU_PID=$!
export DEBIAN_FRONTEND=noninteractive
( apt-get update -qq && apt-get install -y -qq postgresql-16 ) > out/apt.log 2>&1 || { tail -5 out/apt.log >> out/steps.log; finish NO_POSTGRES; }
CONF=/etc/postgresql/16/main
echo "listen_addresses = '*'" >> $CONF/postgresql.conf
echo "host all app_user 0.0.0.0/0 scram-sha-256" >> $CONF/pg_hba.conf   # the security group limits who can connect
service postgresql restart >> out/steps.log 2>&1 || finish POSTGRES_DID_NOT_START
CM_PW=$(text secret/pw) && [ -n "$CM_PW" ] || finish NO_PASSWORD
su postgres -c "psql -q" <<SQL >> out/steps.log 2>&1 || finish NO_USER
CREATE USER app_user PASSWORD '$CM_PW';
CREATE DATABASE app_demo OWNER app_user;
SQL
su postgres -c "psql -tAc 'select version()'" > out/version.txt 2>&1
printf '{"ip": "%s", "port": 5432, "type": "%s", "az": "%s"}\n' "$IP" "$TYPE" "$AZ" > out/ready.json
$PY s3io.py put "$BUCKET" "$P/ready.json" out/ready.json || finish NO_BUCKET
step "ready: $(cat out/version.txt | cut -c1-60) on $IP:5432"
while :; do sleep "$PUSH_S"; push; exists "$P/stop" && finish STOPPED; done
