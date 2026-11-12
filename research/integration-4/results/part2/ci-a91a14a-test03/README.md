# CI a91a14a: test_03_restore test_other_databases_are_untouched_byte_for_byte (sealed), by1 changed during main's restore

- What changed under tenants/by1/: gc/manifest.boundary and gc/compactions.boundary added (19:45:31.87); SlateDB
  manifests 1-4, compactions 1-2 and our versions fx/manifest 1-2 deleted; the newest of each kept. That is the
  background GC of a CLOSED database: the maintenance loop collects a database 30 s after its last use
  (mongo-server/src/main.rs Maintenance::start, gc_after 30 s): SlateDB's GC (gc_tenant_closed, the boundary files)
  and, since perf-bytes, fx::gc::gc_closed (our versions; retention 0 since the coordinator's decision). by1 closed at
  second 0.1 of the test; its GC ran at 31.9 s; the restore of main ran from 22.7 s to 32.9 s. The restore did not
  trigger it and wrote nothing under by1.
- Why only now: the test got longer. Each close of a database whose compactor runs waits the full 10 s join timeout
  of Compactor::stop (TIMING stop_fxc ms=10000-10003): once in this test on fa4649b (part 1, local), twice on a91a14a
  (local and CI: by2 and main), so on a slower runner the window crossed the 30 s GC delay. In the CI server logs of
  a91a14a, 49 of 78 TIMING stop_fxc lines are 10.0 s (ci-a91a14a-stop_fxc-lines.txt): the compactor's task does not
  end on stop and is left detached after 10 s. Not fixed here (the bounded stop is decided with perf-latency).
- Rule 8: can that GC delete anything by1 needs? Its rules keep the newest SlateDB manifest and what it references,
  and our newest version, every pair, reference and fork, and the retention window (fx/gc.rs gc_closed); by1 has no
  reader while closed. Tested: after the restore, after by1's GC has certainly run (its boundary file is there), and
  after a restart of the server, every document of by1 and by2 is read back equal (dump_db).
- Test change (deterministic): gc_only_changes() accepts only GC's own changes under another database (a gc/ boundary
  file added or rewritten; a numbered manifest, compactions file or version deleted while a newer one remains);
  anything else (an object added or rewritten, the newest deleted) fails. A server-free test checks it accepts the CI
  case and catches a write, a rewrite and the newest deleted.
- Local, a91a14a code: the test 1 passed sealed2-derive (36 s) and off (36 s); the server-free test passes; the whole
  test_03 file 12 passed (sealed2-derive).
