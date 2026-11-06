# Return prediction (Fable olix-impossible idea C)

Question: if every night we put on the waiter's disk the data of the q% of users most likely to come back the next
day, what share h of the next day's first accesses (session starts, after 30 minutes of silence) falls on them?
Prompt: fenvix-brain `prompts/P-return-prediction-dig.md`. Report: fenvix-brain
`sessions/2026-11-06-return-prediction/REPORT.md`.

Spend: $0. Everything runs locally; raw data stays outside git (default `/home/user/data`, override with `RP_DATA`).

## Files

- `sim.py`: sessions (30-minute gap), nightly simulation, the predictors, the existing-cache baseline (user-level LRU).
- `loaders.py`: one loader per dataset.
- `run.py <dataset>`: simulate one dataset (pooled, and one database per app or channel) -> `results/<dataset>.json`.
- `summarize.py`: `results/summary.txt` from all JSON files.
- `test_sim.py`: sanity tests on synthetic traces (daily users are found, memoryless users give h close to q,
  no future leak, session split, LRU distance). Run `python3 test_sim.py`.
- `fetch_irc.sh`: downloads the Ubuntu IRC logs used here.

## Data (where it came from, 2026-11-06)

- LSApp: `git clone https://github.com/aliannejadi/lsapp`, then `tar xzf lsapp.tsv.gz` into `$RP_DATA/lsapp/`
  (the `.gz` is a tar archive). md5 of `lsapp.tsv`: 67f4e7fda2c3edcdcebe0245b333c3fa.
- Ubuntu IRC: `./fetch_irc.sh $RP_DATA/irc 2012-01-02 2012-04-29 ubuntu ubuntu-devel ubuntu-server kubuntu ubuntu-uk
  ubuntu-de launchpad-dev ubuntu-kernel juju ubuntu-bugs ubuntu-motu` (ubuntu-offtopic is not logged: 11 channels,
  1,019,976 messages, 79 MB). All 11 channels form the pooled database; launchpad-dev (112 users) is under the
  150-user floor for its own per-channel database, so the spread covers 10 channels.
- CDNOW: `pip install lifetimes` (0.11.3); the loader reads `lifetimes/datasets/CDNOW_master.txt`
  (md5 37ee2da5127d4a369d6fe0052678624d).
- REES46 (side check only, 12.7 days): `git show origin/riscaldamento-utente:proto/riscaldamento-utente/tracce/ecom/ecom-eventi.tsv.gz > $RP_DATA/rees46/ecom-eventi.tsv.gz`.

## Run

    pip install scikit-learn lifetimes
    for d in rees46_m24 lsapp cdnow irc; do OMP_NUM_THREADS=1 python3 run.py $d; done
    python3 summarize.py

About 10 minutes on 4 cores (CDNOW has 517 nights with the learned models refit every 7 nights).

## Definitions

- Night boundary: the UTC hour with the fewest accesses in the dataset. Day n = the 24 hours after boundary n.
- Warm-up: the first 28 days are history only (7 days for the 12.7-day REES46 subset).
- Population "known": every user seen before the boundary (the users who have data in the database).
  Population "active28": only users seen in the last 28 days (stricter: q% of a smaller set).
- h: session starts of selected users on day n / session starts of all known users on day n, summed over nights.
  Brand-new users (first access on day n) have no stored data to read cold; h including them is printed too.
- Predictors: p1 sessions yesterday (tie: most recent); p2 sessions in the last 7 days (tie: active days, recency);
  p3 share of the last 4 same weekdays with a session (tie: active days in 28, recency; with one fill per night
  the hour band adds nothing, the fill covers all 24 hours); p4 per-user gap histogram, P(next session within 24 h
  given the time already elapsed), smoothed towards a pooled prior that includes the open gap of every user;
  p5 logistic regression (P(active)) and gradient boosting with Poisson loss (expected sessions) on the features
  of p1-p4, trained only on earlier nights, refit every 7 nights.
- Existing cache baseline: a user-level LRU that holds 0.2% (or 1%) of all users; `h_after_lru_*` is h measured only
  on the first accesses that this LRU misses (the true cold reads).
- `byte_share`: the selected users' share of all past accesses of known users, a proxy for their share of the bytes.
