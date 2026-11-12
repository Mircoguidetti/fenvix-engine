# first_next.py on our server, local (MinIO on the same machine), integration-4 27014ff (2026-11-12)

The binary of step9/local/builds/build-27014ff-x86_64.json (mongo-server f684c6b1...), product defaults. load_fn.py loads
one Atlas-shaped database (messaggi 12,000, eventi 18,000, ordini 3,600 documents, 300 customers, the cliente_ts index)
and writes its state file (every third customer reserved); the server is stopped and started again (cold caches); then
`first_next.py out --uri ... --states states --customers 40`. Exit 0.

MEASURED (40 customers, all cold at their first request):
- FIRST (messaggi latest 20): p50 4.89 ms, p99 36.56 ms; GETs p50 2 (max 21); bytes p50 2,951.
- NEXT 1 (eventi latest 100): p50 5.08 ms; GETs p50 2; x3.82 the warm repeat at p50 (SLOWER).
- NEXT 2 (ordini by stato): p50 3.68 ms; GETs p50 2; x2.77 (SLOWER).
- NEXT 3 (messaggi older page): p50 3.13 ms; GETs p50 1; x2.35 (SLOWER).
- HOT (the first again): p50 1.33 ms, 0 GETs.
So on integration-4 the next requests of a customer still read S3 (1-2 GETs each), as the total review's R1 says; the
fixes are on big-speed. Locally a GET costs well under a millisecond: on S3 each would add its first-byte time.
Small data on purpose (it checks the program, not the engine at 1 TB).
