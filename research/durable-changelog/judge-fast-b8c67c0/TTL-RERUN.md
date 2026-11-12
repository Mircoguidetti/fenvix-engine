# The three TTL differences of the fast judge on b8c67c0

The fast judge reported 409/413 in diff-py and diff-node. Besides the known `bucketauto.infinity`, it reported three TTL tests: `ttl.large_batch`, `ttl.drop_and_recreate_ttl_index` and `ttl.index_created_on_existing_expired_docs`. In all three, our server had already deleted the expired documents and the reference mongod had not.

The cause is on the reference side. The reference mongod on port 27020 had been started at 17:44:59 without `--setParameter ttlMonitorSleepSecs=1`, so `getParameter` returned 60. `judge/run.sh` `env_up` reuses any mongod already listening on 27020 and does not check that parameter.

After `setParameter ttlMonitorSleepSecs=1` on that mongod, each test was run again with `run_py.py --only <name>` against the same servers. `ttl.large_batch`, `ttl.drop_and_recreate_ttl_index`, `ttl.index_created_on_existing_expired_docs` and `ttl.expired_docs_removed` each gave 1/1 passed.
