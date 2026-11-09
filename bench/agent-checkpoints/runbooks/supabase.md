# Supabase (managed Postgres)

Read `README.md` first.

## What the founder creates (supabase.com/dashboard)

1. Organization on the **Pro** plan (needed for a compute size above Micro; monthly fee UNKNOWN on 2026-11-09,
   supabase.com/pricing not opened).
2. New project `app-demo`, region **West EU (Ireland), eu-west-1** (Supabase runs on AWS and offers Ireland as far as we
   know; confirm in the region list), database password from a password manager.
3. Compute size: **Large** (2 dedicated ARM cores, 8 GB as far as we know; to verify on the page): the size that matches
   our 2 vCPU 8 GB. Not Micro/Small/Medium, which share CPU.
4. Connection string: Settings > Database > **Session mode** (port 5432) or the direct connection, `sslmode=require`, into
   `BENCH_PG_URI_SUPABASE`. The saver tables go into schema `public` of database `postgres` (Supabase's default).
5. Disable unused services for fairness (none needed: the bench only uses Postgres).

## Spend estimate (ESTIMATED, 3 hours)

- Compute: Large x 3 h x UNKNOWN $/h (hourly compute billing, to verify), plus the Pro plan fee if not already paid.
- Storage: under 8 GB included (DECLARED: $0.125/GB above 8 GB); about 1.8 GB stored, so $0.
- Cap: $10 plus the plan fee.

## Teardown and check

1. Project Settings > General > Delete project. Then set compute back if the organization keeps other projects.
2. Check: the project list has no `app-demo`; Usage page the next day shows no compute hours after the deletion.
