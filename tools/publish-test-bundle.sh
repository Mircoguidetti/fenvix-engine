#!/usr/bin/env bash
# Build the release binaries at an engine commit and publish them, with that commit's ctl tests, as a test bundle in the
# PUBLIC repository fenvix-tests, where GitHub Actions runs the full ctl set in parallel (free on public repositories).
#
# Usage: tools/publish-test-bundle.sh COMMIT [--name NAME] [--mode M] [--src DIR] [--no-push] [--force] [--wait]
#   COMMIT      engine commit (any name git understands)
#   --mode M    what the automatic run tests: sealed2-derive (default, the judge's JUDGE_SEALED_RUNS=2 JUDGE_SEALED_DERIVE=1),
#               off (FENVIX_SEALED_RUNS=off), or both
#   --name      bundle name (default: the commit's 7-character short hash)
#   --src DIR   build from this existing checkout instead of a new worktree (its HEAD must be COMMIT, with no engine changes)
#   --no-push   build and check the bundle, print where it is, push nothing
#   --force     replace an existing bundle of the same name
#   --wait      after the push, wait for the run (up to 150 minutes) and print its summary (fenvix-tests ci/result.sh);
#               exit 0 if every file passed
#
# What becomes public, and nothing else: the two binaries, stripped (target/release/mongo-server, proto pq); the commit's
# ctl-tests/*.py; tools/lease-stall/{blockproxy,pathproxy}.py (test_31 imports them); mongo-tests/node/package.json and
# package-lock.json (test_40 runs the Node driver). No engine source. Every file and the binaries' strings are scanned for
# keys, tokens, private keys and internal names before the push; a hit stops the script.
#
# Where: the branch bundle/<NAME> of fenvix-tests, an orphan branch with MANIFEST.txt (commit, toolchain, the
# sha256 of every file) and bundle.tar.gz (and a copy of main's workflow, so that the push starts it). Why a branch and not a release asset: a session can push to the repository with
# git and the access it already has; uploading a release asset needs a GitHub token with API write access, which sessions
# do not have. One orphan branch per bundle keeps main small, and deleting the branch deletes the bundle.
# Pushing the branch starts the workflow (the --mode given); fenvix-tests' ci/result.sh NAME reads the result.
set -euo pipefail
ENGINE=$(cd "$(dirname "$0")/.." && pwd)
TESTS_REPO=${FENVIX_TESTS_REPO:-https://github.com/Mircoguidetti/fenvix-tests}
COMMIT=${1:?usage: tools/publish-test-bundle.sh COMMIT [--name NAME] [--mode M] [--src DIR] [--no-push] [--force]}; shift
NAME=""; SRC=""; PUSH=1; FORCE=0; WAIT=0; MODE=sealed2-derive
while [ $# -gt 0 ]; do
  case $1 in
    --name) NAME=$2; shift 2 ;;
    --mode) MODE=$2; shift 2 ;;
    --src) SRC=$(cd "$2" && pwd); shift 2 ;;
    --no-push) PUSH=0; shift ;;
    --force) FORCE=1; shift ;;
    --wait) WAIT=1; shift ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
done
FULL=$(git -C "$ENGINE" rev-parse --verify "$COMMIT^{commit}")
NAME=${NAME:-${FULL:0:7}}
case $MODE in sealed2-derive|off|both) ;; *) echo "--mode is sealed2-derive, off or both"; exit 2 ;; esac
[[ $NAME =~ ^[A-Za-z0-9._-]+$ ]] || { echo "bundle name must be letters, digits, dot, dash, underscore"; exit 2; }
WORK=$(mktemp -d "${TMPDIR:-/tmp}/test-bundle-$NAME.XXXX")
echo "bundle $NAME from engine commit $FULL, work dir $WORK"

# 1. source at the commit
if [ -n "$SRC" ]; then
  [ "$(git -C "$SRC" rev-parse HEAD)" = "$FULL" ] || { echo "$SRC is not at $FULL"; exit 1; }
  [ -z "$(git -C "$SRC" status --porcelain -- mongo-server src proto Cargo.toml Cargo.lock)" ] || { echo "$SRC has engine changes"; exit 1; }
else
  SRC="$WORK/src"; git -C "$ENGINE" worktree add -q --detach "$SRC" "$FULL"
  trap 'git -C "$ENGINE" worktree remove --force "$SRC" 2>/dev/null || true' EXIT
fi

# 2. build
(cd "$SRC" && cargo build --release -p mongo-server)
(cd "$SRC/proto/quaderno" && cargo build --release --bin pq)

# 3. stage: the binaries (stripped) and only the listed test files, taken from the commit (tracked files only)
ST="$WORK/stage"; mkdir -p "$ST/target/release"
install -m 755 "$SRC/target/release/mongo-server" "$ST/target/release/mongo-server"
install -m 755 "$SRC/proto/quaderno/target/release/pq" "$ST/target/release/pq"
strip "$ST/target/release/mongo-server" "$ST/target/release/pq"
PUBLIC_PATHS=$(git -C "$ENGINE" ls-tree -r --name-only "$FULL" -- ctl-tests tools/lease-stall mongo-tests/node \
  | grep -E '^ctl-tests/[^/]+\.py$|^tools/lease-stall/(blockproxy|pathproxy)\.py$|^mongo-tests/node/package(-lock)?\.json$')
git -C "$ENGINE" archive "$FULL" $PUBLIC_PATHS | tar -x -C "$ST"

# the tests' comments cite internal reports and decisions by path or date: those citations are replaced (comments and
# docstrings only; no code line cites them, and the scan below stops the script if one is left)
(cd "$ST" && sed -i -E 's#\(?(fenvix-brain )?sessions/20[0-9]{2}-[0-9]{2}-[0-9]{2}-[A-Za-z0-9._/-]*\)?#(an internal report)#g; s#DECISIONS 20[0-9]{2}-[0-9]{2}-[0-9]{2}#an internal decision#g' ctl-tests/*.py)

# 4. scan: keys, tokens, private keys, internal names. The tests' own fixed values (minioadmin, the test token, test
#    passwords, FAKEKEY of the fake metadata service) are expected and listed in the report.
PAT='AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_secret_access_key *= *[A-Za-z0-9/+]{40}|-----BEGIN [A-Z ]*PRIVATE KEY|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_|xox[abprs]-|sk-ant-|sk-[A-Za-z0-9]{40,}|AIza[0-9A-Za-z_-]{35}|ip-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}|[a-z0-9-]+\.compute\.internal|[a-z0-9-]+\.(corp|lan)\b|amazonaws\.com/[a-z0-9-]*fenvix|fenvix-brain|supabase\.co|vercel\.app|mircoguidetti[0-9]|@gmail\.com'
# citations of internal reports and decisions: text files only (the server's --help text cites a decision date)
CITE='sessions/20[0-9]{2}-|DECISIONS 20[0-9]{2}'
HITS=$( (cd "$ST" && grep -rIEn "$PAT|$CITE" . || true; for b in target/release/*; do strings "$b" | grep -En "$PAT" | sed "s|^|$b: |" || true; done) )
if [ -n "$HITS" ]; then echo "SCAN FAILED, nothing pushed:"; echo "$HITS" | head -50; exit 1; fi
echo "scan: no key, token, private key or internal name found"

# 5. tarball and manifest
(cd "$ST" && find . -type f | sed 's|^\./||' | sort > "$WORK/files.txt" && tar -czf "$WORK/bundle.tar.gz" -T "$WORK/files.txt")
{
  echo "bundle: $NAME"
  echo "engine_commit: $FULL"
  echo "built_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "run_modes: $MODE"
  echo "rustc: $(rustc --version)"
  echo "cargo: $(cargo --version)"
  echo "build: cargo build --release -p mongo-server (repository root); cargo build --release --bin pq (proto/quaderno); then strip both"
  echo "os: $(. /etc/os-release && echo "$PRETTY_NAME"), glibc $(ldd --version | head -1 | awk '{print $NF}')"
  echo "tarball:"
  (cd "$WORK" && sha256sum bundle.tar.gz)
  echo "files:"
  (cd "$ST" && xargs -a "$WORK/files.txt" -d '\n' sha256sum)
} > "$WORK/MANIFEST.txt"
grep -E "target/release/" "$WORK/MANIFEST.txt"
echo "$(wc -l < "$WORK/files.txt") files, tarball $(du -h "$WORK/bundle.tar.gz" | cut -f1)"
if [ $PUSH = 0 ]; then echo "not pushed: $WORK/MANIFEST.txt $WORK/bundle.tar.gz"; exit 0; fi

# 6. push the orphan branch bundle/<NAME>
if git ls-remote --exit-code "$TESTS_REPO" "refs/heads/bundle/$NAME" > /dev/null 2>&1 && [ $FORCE = 0 ]; then
  echo "bundle/$NAME already exists in $TESTS_REPO (use --force to replace it)"; exit 1
fi
# GitHub runs the workflow file of the pushed commit, so the bundle carries a copy of main's workflow (the jobs take
# their scripts from main)
R="$WORK/repo"; git clone -q --depth 1 --branch main "$TESTS_REPO" "$WORK/main"
git init -q "$R"; git -C "$R" checkout -q --orphan "bundle/$NAME"
mkdir -p "$R/.github/workflows"; cp "$WORK/main/.github/workflows/ctl.yml" "$R/.github/workflows/"
cp "$WORK/MANIFEST.txt" "$WORK/bundle.tar.gz" "$R/"
git -C "$R" add MANIFEST.txt bundle.tar.gz .github/workflows/ctl.yml
git -C "$R" -c user.name="fenvix test bundle" -c user.email="bundle@users.noreply.github.com" commit -q -m "bundle $NAME: engine $FULL"
git -C "$R" push $([ $FORCE = 1 ] && echo --force) "$TESTS_REPO" "bundle/$NAME:refs/heads/bundle/$NAME"
echo "pushed bundle/$NAME; the workflow starts on its own (mode $MODE). Result: ci/result.sh $NAME in fenvix-tests"
if [ $WAIT = 1 ]; then bash "$WORK/main/ci/result.sh" "$NAME" --wait; exit $?; fi
exit 0
