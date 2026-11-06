FIX 3 — corrected headline: Ubuntu IRC, q = 8% of users, brand-new users counted as misses: h = 60.6% (p5_lr), 57.0% (p2_last7) [MEASURED; owner 76.4% / 72.2%]. That selection needs about 600 GB at 1 TB, not 82 GB [ESTIMATED]. With a disk quota that really holds 8% of the data (82 GB), h = 9.5% (p5_lr), best 11.1% (p1), random 7.9% [MEASURED, bytes proxy = accesses]. IRC savings: 11.7 $ quiet / 46.9 $ medium per 1 TB customer per month with the ~600 GB disk; 1.2-1.4 $ / 4.7-5.7 $ with 82 GB, below the 9.5 $ night pass [ESTIMATED].

# Review: return-prediction (fenvix-engine c95028b, `research/return-prediction/`)

Reviewer branch: `review-return-prediction` (based on c95028b). Spend: $0, no AWS.

## Verdict

- **The contract as written (q = % of USERS): CONTINUE still holds.** IRC at q = 8% is 60.6% even with new users
  counted as misses (≥ 35%). No leakage was found.
- **But the disk the study assumes cannot hold that selection.** The users the predictors pick are the heavy ones: they own
  58.5% of past accesses on IRC. With the quota measured in bytes (8% of 1 TB = 82 GB, the figure in the delivery), the
  same contract gives **MIXED**: LSApp 27.0% (p5_gbm), CDNOW 15.5% (p3), IRC 9.5-11.1% (random 7.9%) [MEASURED]. On IRC
  the predictors barely beat random at a fixed byte budget.
- The dollar case is weaker than claimed in both readings (see FIX 3). The coordinator should not carry "15 $ / 62 $"
  forward.

## Data obtained (2026-11-06)

All four datasets, same sources as the README:
- LSApp from GitHub: md5 `67f4e7fd…` matches.
- Ubuntu IRC via `fetch_irc.sh`: 11 channels, 1,019,976 messages loaded, same as the owner.
- CDNOW from `lifetimes` 0.11.3: md5 `37ee2da5…` matches.
- REES46 subset from `origin/riscaldamento-utente`.

## Reproduction [MEASURED]

`run.py` re-run unchanged in a scratch copy. Every headline number reproduces exactly:

| Dataset | Predictor | Owner | Re-run |
|---|---|---|---|
| IRC | p5_lr | 76.4% | 76.4% |
| IRC | p2_last7 | 72.2% | 72.2% |
| LSApp | p5_gbm | 49.7% | 49.7% |
| CDNOW | p5_lr | 41.5% | 41.5% |
| REES46 | p2_last7 | 30.6% | 30.6% |

The owner's `test_sim.py` passes (5/5).

## Leakage (priority 1): none found

`review/test_review.py::test_no_future_leak_selections[False|True]` (PASS) deletes every access from day 38 on and checks
that nights 28-36 select exactly the same users. It covers all 5 predictors including the refit logistic regression and
GBM, every q, and both populations. This is stronger than the owner's test, which only compared four features.

`test_no_future_leak_known_population` (PASS) checks that q is taken over users seen before the boundary only.

Code read checks:
- Features use `st < T` only.
- The LR/GBM training rows are appended after scoring, so night n trains on nights < n. The scaler is fit on training
  rows only.
- Sessions are split over the whole trace, but a session start only depends on the 30 minutes before it.
- The q denominator is all known users (honest, not active-only).

Residual whole-trace choices are in NOTE 1.

## Defects (each changes a number; tests in `research/return-prediction/review/test_review.py` FAIL on c95028b)

Run with `cd research/return-prediction && python3 -m pytest -q review/test_review.py`.

1. **Disk quota ignores who is selected** — `test_defect_disk_quota_ignores_who_is_selected`.
   - `summarize.py` sizes the q = 8% fill as 8% × 1 TB = 82 GB, as if the selected users were average.
   - The study's own `byte_share` says the IRC p5_lr selection owns 58.5% of past accesses: about 599 GB at 1 TB
     [ESTIMATED, accesses as the bytes proxy]. For LSApp and CDNOW the same figure is 20-31%, i.e. 210-315 GB.
   - Why it matters: the h numbers are bought with 2.5-7× the disk the delivery states.
   - Reproduction: `review/audit.py <dataset> --bytes` re-runs the same rankings under a real 8% byte budget. Results at
     q = 8% of bytes [MEASURED]:

     | Dataset | Predictors | Random | Most active ever |
     |---|---|---|---|
     | IRC | 9.1-11.1% | 7.9% | 7.8% |
     | LSApp | p1 26.6%, p5_gbm 27.0% | 8.2% | 2.9% |
     | CDNOW | 13.8-15.5% | 8.1% | 12.6% |

2. **Brand-new users are not counted as misses in the headline** — `test_defect_headline_h_excludes_brand_new_users`.
   - The brief requires users first seen on day N+1 to count as misses. The CONTINUE line, `summarize.best` and the
     dollars all use `h` over known users only. `h_incl_new_users` is computed but only printed on the side.
   - Corrected values at q = 8% [MEASURED]:

     | Dataset | Owner h | h with new users as misses |
     |---|---|---|
     | IRC p5_lr | 76.4% | 60.6% |
     | IRC p2_last7 | 72.2% | 57.0% |
     | LSApp p5_gbm | 49.7% | 46.2% |
     | CDNOW p5_lr | 41.5% | 31.0% |
     | REES46 p2_last7 | 30.6% | 16.2% |

   - Why it matters: CDNOW falls under 35%, so IRC is the only dataset over the CONTINUE line.
   - The owner argues new users have no stored data. That does not hold for IRC: "first seen" means first message
     inside a 17-week window, not sign-up. It does not hold for LSApp either: that is when the trace starts.

3. **Dollars use h over all session starts** — `test_defect_dollars_use_h_not_cold_reads`.
   - The reference model prices cold-read GETs (21 / 84 $ per 1 TB customer per month). It should be multiplied by the
     share of cold reads saved: hits after the existing 0.2% user LRU, with new-user sessions in the denominator.
   - `summarize.py` prints 16.0 / 64.1 $ for IRC; the owner's 15 / 62 $ is h-after-LRU without new users.
   - Corrected values [ESTIMATED]:

     | Dataset | Disk | Quiet | Medium |
     |---|---|---|---|
     | IRC p5_lr | ~600 GB (q = 8% of users) | 11.7 $ | 46.9 $ |
     | LSApp p5_gbm | q = 8% of users | 9.1 $ | 36.5 $ |
     | CDNOW p5_lr | q = 8% of users | 6.3 $ | 25.2 $ |
     | IRC | 82 GB (8% of bytes) | 1.2-1.4 $ | 4.7-5.7 $ |
     | LSApp | 82 GB (8% of bytes) | 5.4 $ | 21.6 $ |

   - At 82 GB every dataset saves less than the 9.5 $ night pass in the quiet case. The 9.5 $ is DECLARED (see NOTE 6).

## Baselines (priority 3) [MEASURED, q = 8% of users, known users]

| Dataset | Random | Most active ever | Best simple | Best learned |
|---|---|---|---|---|
| IRC | 8.1% | 68.7% | p2 72.2% | p5_lr 76.4% |
| LSApp | 8.6% | 23.2% | p1 48.4% | p5_gbm 49.7% |
| CDNOW | 8.1% | 40.5% | p1/p3 33.9% | p5_lr 41.5% |

Random lands on q, which confirms the metric is sane. **On CDNOW, "most active ever" beats every simple predictor and
is 1 point under the logistic regression.** The CDNOW 41.5% says "frequent buyers buy again", not that next-day
prediction works. On IRC the predictors add 3.5-7.7 points over "most active ever". On LSApp they add about 25 points:
the only dataset where recency clearly matters.

## Arithmetic checked against the cited inputs (`summarize.py` constants, reference model unchanged)

- Free NVMe: 118 − 44 × 1.30 / 0.85 = 50.7 GB. **Correct.**
- 8% × 1024 GB = 81.9 GB. **Correct as arithmetic, wrong as a quota** (FIX 1).
- 0.764 × 21 = 16.0 and 0.764 × 84 = 64.1: these match `summary.txt`. The owner's "15 / 62" is 0.735 (after LRU)
  × 21 / 84. That mix of sources is FIX 3.

## Notes (do not change the verdict)

1. **Whole-trace choices.**
   - The night boundary hour is the quietest hour of the WHOLE trace (`boundary_hour`).
   - Sessions are split over the whole trace. The leak test fixes the hour.
   - Neither feeds a ranking with future behaviour. In production, use a trailing window for the hour.
2. **h is session-weighted.** A user with 5 sessions on day N+1 counts 5 times. One fill per user per day mostly helps
   the first session; later ones are already in RAM. On IRC, h first-of-day is 64.8% vs h 76.4%. The LRU-adjusted
   figure in FIX 3 covers part of this.
3. **q is relative to users seen inside a 17-week window.** On IRC, q = 8% is 1,487 users for about 695 daily active
   ones (2.1×), so the oracle reaches 100% at q = 5%. A real database with years of dormant users makes q = 8% a much
   larger slot count. The study cannot say whether h then rises or the byte gap grows.
4. **The bytes proxy is "accesses"** (IRC messages, LSApp app events, CDNOW purchase days). If a real user's stored
   bytes do not grow with activity, FIX 1 shrinks. This is UNKNOWN until a real per-user size distribution is measured.
   It is the single most important follow-up.
5. In `review/audit.py --bytes`, the column "mean selected users" prints the nominal k, not the number selected under
   the byte budget. `byte_share` (~8%) is the correct check.
6. The D1c `cost.py` and the 9.5 $ night-pass figure live in fenvix-brain, not in this repository. The 9.5 $ is
   DECLARED, not checked.
7. REES46 (side check, 5 nights): with new users as misses, h = 16.2%. Brand-new users make 89% as many sessions as
   known ones there.

## Files added on `review-return-prediction`

- `research/return-prediction/review/test_review.py`: 3 leak tests (PASS) and 3 defect tests (FAIL on c95028b).
- `research/return-prediction/review/audit.py`: baselines, byte budget, and dollars with new users as misses.
- `research/return-prediction/review/results/*.json`: audit output. IRC includes the learned models. CDNOW and the
  LSApp user budget run with `--no-learned`; their learned numbers are the owner's, reproduced exactly.
