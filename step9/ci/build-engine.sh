#!/usr/bin/env bash
# Step 9 · build the engine's binaries for the machines, reproducibly: the same commit with the same toolchain gives the
# same bytes on any machine (the recipe of fenvix-tests ci/build-engine.sh at 0ff5bc2, MEASURED there: GitHub runners
# agree on one binary). Used by the workflow's build job (.github/workflows/step9-kitchen.yml) and by the local round
# (step9/local/local-run.sh), so the binary the local benchmark measured and the binary the AWS machines run can be
# compared by sha256 (the gate checks the local side; the AWS summary records the other).
#   1. one fixed folder, FX_BUILD_DIR (default /tmp/fx-src), filled from the checkout with `git archive` (tracked files
#      only, as at the commit): cargo mixes each workspace package's absolute folder into symbol names, so two folders
#      give two binaries even with path remapping;
#   2. RUSTFLAGS --remap-path-prefix for that folder, the cargo registry and the toolchain (no path of this machine in
#      the binary);
#   3. cargo build --release --locked -p mongo-server, and pq in proto/quaderno; not stripped (backtraces on the machines);
#   4. OUT_DIR/mongo-server-ARCH/{mongo-server,motore-commit.txt}, OUT_DIR/pq-ARCH/pq and OUT_DIR/build-ARCH.json with
#      the commit, toolchain, flags and the sha256 of both binaries.
# Usage: build-engine.sh ENGINE_DIR EXPECTED_COMMIT OUT_DIR   (RUST_TOOLCHAIN in the environment: cargo +$RUST_TOOLCHAIN)
set -euo pipefail
SRC=$(cd "$1" && pwd); WANT=$2; mkdir -p "$3"; OUT=$(cd "$3" && pwd)
E=${FX_BUILD_DIR:-/tmp/fx-src}
ARCH=$(uname -m)
HEAD=$(git -C "$SRC" rev-parse HEAD)
[ "$HEAD" = "$WANT" ] || [ "${HEAD:0:${#WANT}}" = "$WANT" ] || { echo "build-engine: checkout is at $HEAD, not $WANT"; exit 1; }
TC=(); [ -n "${RUST_TOOLCHAIN:-}" ] && TC=("+$RUST_TOOLCHAIN")
CARGO_HOME_DIR=${CARGO_HOME:-$HOME/.cargo}; RUSTUP_HOME_DIR=${RUSTUP_HOME:-$HOME/.rustup}
export RUSTFLAGS="--remap-path-prefix=$E=/src --remap-path-prefix=$CARGO_HOME_DIR=/cargo --remap-path-prefix=$RUSTUP_HOME_DIR=/rustup"
export CARGO_TERM_COLOR=never
if [ "$E" != "$SRC" ]; then
  rm -rf "${E:?}"; mkdir -p "$E"
  git -C "$SRC" archive "$HEAD" | tar -x -C "$E"
fi
(cd "$E" && cargo "${TC[@]}" build --release --locked -p mongo-server)
(cd "$E" && cargo "${TC[@]}" build --release --locked --manifest-path proto/quaderno/Cargo.toml --bin pq)
mkdir -p "$OUT/mongo-server-$ARCH" "$OUT/pq-$ARCH"
cp "$E/target/release/mongo-server" "$OUT/mongo-server-$ARCH/"
echo "$HEAD" > "$OUT/mongo-server-$ARCH/motore-commit.txt"
cp "$E/proto/quaderno/target/release/pq" "$OUT/pq-$ARCH/"
python3 - "$HEAD" "$OUT" "$ARCH" "$RUSTFLAGS" "${RUST_TOOLCHAIN:-}" <<'Q'
import hashlib, json, subprocess, sys
head, out, arch, flags, tc = sys.argv[1:]
sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()
plus = [f"+{tc}"] if tc else []
info = {"engine_commit": head, "arch": arch,
        "rustc": subprocess.run(["rustc", *plus, "--version"], capture_output=True, text=True).stdout.strip(),
        "cargo": subprocess.run(["cargo", *plus, "--version"], capture_output=True, text=True).stdout.strip(),
        "build": "fixed folder (FX_BUILD_DIR, default /tmp/fx-src) from git archive; cargo build --release --locked -p mongo-server; "
                 "proto/quaderno: cargo build --release --locked --bin pq; not stripped",
        "rustflags": flags,
        "mongo_server_sha256": sha(f"{out}/mongo-server-{arch}/mongo-server"), "pq_sha256": sha(f"{out}/pq-{arch}/pq")}
json.dump(info, open(f"{out}/build-{arch}.json", "w"), indent=1)
for k, v in info.items():
    print(f"{k}: {v}")
Q
