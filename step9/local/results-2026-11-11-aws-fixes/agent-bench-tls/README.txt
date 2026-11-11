Local validation (aws-fixes, 2026-11-11), NOT results: tiny profile, everything on one 4-vCPU sandbox.
pgbox: step9/machine/pgbox.sh run unchanged (only common.sh replaced by a file-based double) in an ubuntu:24.04
container: PostgreSQL 16.15 installed, ssl on, hostssl only for app_user, shared_buffers = 25% of the memory the machine
reports (in docker /proc/meminfo shows the host's 16 GB, so 4021 MB here; an m7i.large has 8 GiB: about 2 GB),
pg_settings saved (pg_settings.csv, ready.json says "single node, no copy", TLS required). A connection with
sslmode=disable is refused ("no pg_hba.conf entry ... no encryption").
agent client: step9/programs/agent_bench.py with mongod 7.0 standing in for our cook (the code path of the fenvix
target is the MongoDB saver) and the pgbox container as the Postgres machine: both targets ok, the postgres summary
says durability "single node, no copy (NOT equal durability ...)", pg_settings.json with ssl_in_use true.
A second run with --aurora-uri pointing at the same Postgres (a stand-in: aurora_version() does not exist there) ran
the "aurora" target FIRST, then tried to write agent/aurora-done (no S3 here: EndpointConnectionError, printed, the
benchmark goes on; run.py deletes Aurora at the end anyway), then ours, then the Postgres machine.
aurora-standin-pg_settings.json: after the fix that guards aurora_version() on its own, the settings are kept.
