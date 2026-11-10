#!/usr/bin/env bash
# Trial merge of c2-delta-log (3e899b0) into c2-step8-migration (c9871fc, which carries step 4, 6, 7a and 7b). Eight
# files conflict (both sides added lines); files/ holds them resolved by keeping both sides, plus two adaptations so it
# compiles with step 6's counter: the derived rebuild reads blocks through 7b's uncached view::blocks(), and the
# prefetch is skipped (answers never depend on either). files/src/fx/derive_tests.rs carries the review's erasure test.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git fetch -q origin c2-delta-log c2-step8-migration
git worktree add /tmp/trial-7b origin/c2-step8-migration && cd /tmp/trial-7b
git -c user.name=review -c user.email=review@localhost merge --no-edit origin/c2-delta-log || true
cp -r "$OLDPWD/research/c2-delta-log/review/trial-merges/step8-7b/files/." .
git show claude/loving-maxwell-kh8xtl:src/fx/derive.rs > src/fx/derive.rs
cargo test --release --lib -- --test-threads 1 --nocapture review_erasure_of_a_derived_thread
