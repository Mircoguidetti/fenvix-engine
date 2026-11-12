Probes of test_52 / test_52r on integration-4 after merge 4 (MEASURED, sealed2+derive, this sandbox):
- probe_52_retry.py (test_52 with the in-process scan retried): merge 1 of 4 runs answered after 16.6 s (5 refusals,
  F7 "serving lease expired"), 3 of 4 never within 60 s; outage-loss alone 5 of 6 at once, 1 of 6 after 9.6 s (2 F7
  refusals; that run's loader reached 450,001 documents). Merge loaders: 635,001 to 755,001 acknowledged; outage-loss
  alone: 150,001 to 165,001 (450,001 once).
- the 20,000-batch variant is refused too: an unfiltered find reads every document before its first batch.
- probe_52_restart.py / probe_52r_restart.py (in-process scan allowed to be refused, restart read-back on a normal-lease
  server): test_52 670,001 acknowledged, 0 missing; 52r mixed writes 183,361 in scan, _id index and k index, OK;
  52r split insertMany 6 acknowledged calls of 120,000, OK.
