# First-course trace test (Fable olix-transport ideas 1 and 2)

Question: who makes the cold first accesses of each day? Brand-new users (idea 2, "zero trips for new users": an
existence filter answers with no GET) or users seen within T days (idea 1, "keep the first course": a ~32 KB opening
kept for T days answers the first query locally)? And how much fast storage do the openings take?

Prompt: fenvix-brain `prompts/P-first-course-traces.md`. Report: fenvix-brain
`sessions/2026-11-06-first-course-traces/REPORT.md`. Spend: $0, local only.

## Files

- `first_course.py`: classification of cold session starts by time since the user was last seen; openings held per
  day for T = 1, 3, 7, 14, 30 days; scaling to a 1 TB customer; spread across users.
- `run.py [irc lsapp cdnow]`: runs the pooled database and one database per channel or app ->
  `results/<dataset>.json` and `results/summary.txt` (every number of the report).
- `test_first_course.py`: synthetic traces with a known answer (all users daily, all users new, users returning only
  after 60 days, LRU hits are not cold, the 1 TB scaling arithmetic). `python3 -m pytest -q test_first_course.py`.

It reuses `../return-prediction/sim.py` (30-minute session rule, quietest-hour night boundary, user-level LRU
distance) and `../return-prediction/loaders.py` unchanged. Data: same sources and checksums as
`../return-prediction/README.md` (LSApp from GitHub, Ubuntu IRC via `fetch_irc.sh`, CDNOW from `lifetimes` 0.11.3).

## Run

    pip install lifetimes numpy pytest
    RP_DATA=/home/user/data python3 run.py      # under a minute

## Definitions

- Session start: an access after at least 30 minutes of silence. Cold: the start misses a user-level LRU holding
  0.2% of all users (the RAM counter at 1 TB, as in return-prediction). Days 0-27 are history only.
- Last seen: end of the user's previous session. "New" = no previous access in the trace (in a short trace this
  includes users silent since before the trace began; the new share in the first and last 28 evaluated days shows
  how much).
- Openings held on a day: distinct users with an access in the T days before the day boundary. Share of known:
  that count divided by the users seen before the boundary, averaged over evaluated days.
- 1 TB scaling: users = 1 TiB / average size per user (64 KB, 512 KB, 4 MB); footprint = share of known x users x
  32 KiB; fits if at most 29 GB (a quarter of the 118 GB NVMe of an m8gd.large).
- Spread: covered starts (cold, last seen within T) per user; share of the top 1% and 10% of covered users.
