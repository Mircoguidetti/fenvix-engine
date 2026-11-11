#!/bin/bash
# this branch merged onto each fix in flight, alone: build, the generator's tests, the fx tests
cd /home/user/trial4
export CARGO_TARGET_DIR=/home/user/work/trial-target
for b in c2-fix-hangs c2-fix-long-reads claude/clone-uploads-fix-hkuyhx c2-fix-outage-loss; do
  git checkout -q -f --detach origin/$b
  if ! git merge --no-edit -q origin/ballast-on-i3 >/dev/null 2>&1; then
    # the one known conflict: the test-module list of src/fx/mod.rs; keep both lines
    sed -i '/^<<<<<<< /d; /^=======$/d; /^>>>>>>> /d' src/fx/mod.rs
    git add src/fx/mod.rs; git -c core.editor=true commit -q --no-edit
    echo "$b: conflict in src/fx/mod.rs solved by keeping both"
  fi
  echo "== $b $(git rev-parse --short origin/$b) + ballast-on-i3 = $(git rev-parse --short HEAD)"
  cargo test --release -q -p tenant-bench --lib fx:: 2>&1 | grep -E "^test result|panicked|error(\[|:)" | head -5
  cargo test --release -q -p mongo-server ballast 2>&1 | grep -E "^test result|panicked|error(\[|:)" | head -5
done
echo TRIAL-DONE
