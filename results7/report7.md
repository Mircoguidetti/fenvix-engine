| Group | pymongo pass/total | Node pass/total |
|---|---|---|
| Old suite, core (in scope of results5/6) | 1081/1081 | 1081/1081 |
| Old suite, extra probes (unsupported features of results5/6) | 37/49 | 37/49 |
| Step 2 — secondary indexes (`idx.`) | 141/141 | 141/141 |
| Step 3 — TTL indexes (`ttl.`) | 42/42 | 42/42 |
| Transactions (`txn.`, replica-set reference) | 42/42 | 42/42 |
| Change streams (`cs.`, replica-set reference) | 47/47 | 47/47 |
| `$where` (`where.`) | 30/30 | 30/30 |
| Step 4 — bitwise query operators and `$bit` (`bits.`) | 28/28 | 28/28 |
| Step 4 — expression operators (`op.`) | 203/203 | 203/203 |
| Step 4 — stages and accumulators (`stage7.`) | 83/83 | 83/83 |
| **All tests** | 1734/1746 | 1734/1746 |

Failing in-scope tests: none
