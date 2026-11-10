#!/usr/bin/env bash
# Trial merge of c2-delta-log (3e899b0) with c2-step5b-reader (bf2f852): no textual conflict. The review's test is
# appended to src/fx/derive_tests.rs (this folder's copy = the owner's file + the planted-bug switches + the test).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git fetch -q origin c2-delta-log c2-step5b-reader
git worktree add /tmp/trial-5b origin/c2-delta-log && cd /tmp/trial-5b
git -c user.name=review -c user.email=review@localhost merge --no-edit origin/c2-step5b-reader
cp "$OLDPWD/research/c2-delta-log/review/trial-merges/step5b/derive_tests.rs" src/fx/derive_tests.rs
# derive.rs needs the review's plant switches (REVIEW_PREV) used by setup(): take it from the review branch
git show claude/loving-maxwell-kh8xtl:src/fx/derive.rs > src/fx/derive.rs
cargo test --release --lib -- --test-threads 1 --nocapture review_wave_reader_rebuilds_derived_documents
