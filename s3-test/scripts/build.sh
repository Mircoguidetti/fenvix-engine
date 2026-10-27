#!/usr/bin/env bash
# Build in Amazon Linux 2023 (same glibc as the instance) BEFORE any AWS resource exists: engine (with
# s3-test/patches/0001) and s3probe, then bundle.tgz = binaries + mongo-tests + s3-test/scripts + prices.json.
set -eu
cd "${GITHUB_WORKSPACE:-.}"; OUT=run-out; mkdir -p "$OUT/bundle/bin" "$OUT/bundle/scripts"
git apply --check s3-test/patches/0001-s3-aws-instance-profile.patch
git apply s3-test/patches/0001-s3-aws-instance-profile.patch
docker run --rm -v "$PWD":/w -w /w public.ecr.aws/amazonlinux/amazonlinux:2023 bash -ec '
  dnf install -y gcc gcc-c++ make cmake perl-core openssl-devel pkgconfig git >/dev/null
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
  . $HOME/.cargo/env
  export CARGO_TARGET_DIR=/w/run-out/target
  cargo build --release --locked -p mongo-server
  (cd s3-test/probe && cargo build --release)
'
cp "$OUT/target/release/mongo-server" "$OUT/bundle/bin/"
cp s3-test/probe/target/release/s3probe "$OUT/bundle/bin/" 2>/dev/null || cp "$OUT/target/release/s3probe" "$OUT/bundle/bin/"
cp -r mongo-tests "$OUT/bundle/mongo-tests"; rm -rf "$OUT/bundle/mongo-tests/out" "$OUT/bundle/mongo-tests/node/node_modules"
cp s3-test/scripts/* "$OUT/bundle/scripts/"; cp s3-test/prices.json "$OUT/bundle/scripts/prices.json"
tar czf "$OUT/bundle.tgz" -C "$OUT/bundle" .
ls -la "$OUT/bundle.tgz"
