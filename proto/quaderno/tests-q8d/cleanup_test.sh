#!/bin/bash
# Q8d test for reviewer C2: the Q8c cleanup deletes only this run's machines and buckets, never those of the other
# AWS tests that share the tag project=fenvix-coldread (Ireland, minimal kitchen and its kept pantry). A fake `aws`
# on PATH answers from a fixed list and logs every delete. Exit 0 = PASS.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
cat > "$T/aws" <<'FAKE'
#!/bin/bash
# machines: id name (all with tag project=fenvix-coldread)
M="i-q8a fenvix-q8-111-r0
i-q8b fenvix-q8-111-gen
i-q8old fenvix-q8-99-r1
i-irl fenvix-irlanda-gen
i-cmd fenvix-cmd-waiter"
B="fenvix-coldread-q8-111 fenvix-coldread-permessi-111 fenvix-coldread-q8-99 fenvix-coldread-cmd-pantry fenvix-coldread-irl-42 fenvix-coldread-permessi-7"
case "$1 $2" in
  "ec2 describe-instances")
    pat=$(printf '%s\n' "$@" | sed -n 's/^Name=tag:Name,Values=//p')
    out=""; while read -r id n; do [[ "$n" == $pat ]] && out="$out $id"; done <<<"$M"; echo $out ;;
  "ec2 terminate-instances") shift 3; echo "TERMINATE $*" >> "$FAKE_LOG" ;;
  "ec2 wait") ;;
  "s3api list-buckets") echo "$B" | tr ' ' '\t' ;;
  "s3 rb") echo "RB $3" >> "$FAKE_LOG" ;;
esac
FAKE
chmod +x "$T/aws"
fail=0
check() { # label, env..., expected deletes
  local label=$1 exp=$2; shift 2
  : > "$T/log"
  env PATH="$T:$PATH" FAKE_LOG="$T/log" "$@" bash "${PULIZIA:-$HERE/../aws-q8c/pulizia.sh}" >/dev/null
  got=$(sort "$T/log" | tr '\n' ';')
  if [ "$got" = "$exp" ]; then echo "PASS $label"; else echo "FAIL $label: got [$got] expected [$exp]"; fail=1; fi
}
check "normal run deletes only run 111" "RB s3://fenvix-coldread-permessi-111;RB s3://fenvix-coldread-q8-111;TERMINATE i-q8a i-q8b;" GITHUB_RUN_ID=111 BUCKET=fenvix-coldread-q8-111
check "smonta deletes every Q8 leftover, never the others" "RB s3://fenvix-coldread-q8-111;RB s3://fenvix-coldread-q8-99;TERMINATE i-q8a i-q8b i-q8old;" GITHUB_RUN_ID=222 Q8_MODO=smonta
grep -q "group: fenvix-m31" "${WORKFLOW:-$HERE/../../../.github/workflows/q8c-quaderno.yml}" && echo "PASS workflow in group fenvix-m31" || { echo "FAIL workflow group"; fail=1; }
exit $fail
