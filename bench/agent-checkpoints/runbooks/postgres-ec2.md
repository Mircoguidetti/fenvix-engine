# Runbook: PostgreSQL on our own EC2 (the stand-in for a Supabase project)

Founder, 2026-11-09: no Supabase account. A Supabase project is one PostgreSQL on one machine with EBS storage, so the
benchmark runs PostgreSQL 16 (Ubuntu 24.04's package) on one EC2 machine with its data on gp3, inside the step 9
workflow (`step9/machine/pgbox.sh`, program `agent-bench`), and prices it with Supabase's declared prices.

- Machine: m7i.large (2 vCPU, 8 GiB, $0.1124/h DECLARED, AWS Price List eu-west-1 2026-11-04), gp3 20 GB, eu-west-1a,
  the same zone as the benchmark client. Within 8 vCPU: cook with log-0 (2) + log-1, log-2 (2) + this machine (2) + the
  agent client c7i.large (2) = 8; the warm spare is off by then.
- Connection: user `app_user` with the round's password (never in a file), database `app_demo`, port 5432 reachable
  only inside the round's security group; TLS off inside the VPC (Supabase uses TLS: a small difference, stated).
- What it does not have that Supabase has: the pooler (Supavisor), backups, the REST layer. The LangGraph Postgres
  saver connects directly, as Supabase's session mode does.
- Prices for the comparison (Supabase's, not ours): storage $0.125 per GB-month above 8 GB (DECLARED, supabase.com/
  pricing, fact check 2026-11-08); compute size and hourly price UNKNOWN (the page was not reachable from the session
  sandbox: fill `prices.json` before the round).
- Spend: the machine for about 1 hour of the agent phase, about $0.12 (ESTIMATED: 1 h x $0.1124 + disk and IPv4).
- Teardown: the runner stops it with the round; `teardown.py` and the final check cover it (tag project=fenvix-coldread).
