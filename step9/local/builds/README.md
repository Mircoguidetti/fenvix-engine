# Engine builds with step9/ci/build-engine.sh (the workflow's recipe), local sandbox, x86_64, rustc 1.97.0

- integration-4 27014ff2e3e3e4ff223aa778746f3dce433c6549 (part 2 head, engine code of 332c687c): two clean builds in
  /tmp/fx-src (the folder emptied before each) gave the same bytes. MEASURED:
  mongo-server f684c6b1d3378821cfd60a1712e0467da5c3cfd8f13965379c5fde8b836f580b,
  pq 997e1f7719fb3c92e771517dc1f3b7a25fcaab650942006e2b02411932bdbf5c (build-27014ff-x86_64.json).
- NOT KNOWN yet: whether a GitHub runner gives the same bytes. The flags remap the cargo and rustup folders to fixed
  names, but those folders differ between this sandbox (/root) and a runner (/home/runner), and RUSTFLAGS is part of
  cargo's metadata hash. fenvix-tests 0ff5bc2 measured agreement between runners only. The first AWS build prints its
  sha256 (job summary and out/build-ARCH.json); compare it with the local benchmark's before the round.
- Not the final engine: perf-latency-on-i4 is still to come; the final head is built the same way.
