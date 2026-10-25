#!/usr/bin/env bash
# Checks one run on the happy path.
#
#   ./scripts/acceptance.sh                    # the default workflow
#   ./scripts/acceptance.sh "Triple Assay"     # a named one, see workflows.yaml
#
# Seven checks must pass. What the instruments did comes from their own status
# (ports 7101-7103), not from the executor.
set -uo pipefail

EXEC=${EXEC:-http://localhost:5002}
S3=${S3:-http://localhost:8343}
INSTRUMENTS=${INSTRUMENTS:-"http://localhost:7101 http://localhost:7102 http://localhost:7103"}
TIMEOUT=${TIMEOUT:-90}
workflow=${1:-}

pass=0; fail=0
ok()   { printf '  PASS  %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  FAIL  %s\n' "$1"; fail=$((fail+1)); }
note() { printf '  ....  %s\n' "$1"; }

sha256() { if command -v sha256sum >/dev/null; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi; }

instruments() {
  local out='[]' one
  for url in $INSTRUMENTS; do
    one=$(curl -sf --max-time 5 "$url/status") || { echo "could not read $url/status" >&2; one='null'; }
    out=$(jq -c --argjson one "$one" '. + [$one]' <<<"$out")
  done
  echo "$out"
}

faults=$(instruments | jq -r '[.[] | select(. != null) | select((.fail_pct // 0) + (.hang_pct // 0) > 0) | "\(.device_id) fail=\(.fail_pct)% hang=\(.hang_pct)%"] | join(", ")')
if [ -n "$faults" ]; then
  echo "NOTE: instrument faults are switched on ($faults)."
  echo "      Checks below may fail for that reason. Set them back to 0."
  echo
fi

body='{}'
[ -n "$workflow" ] && body=$(jq -nc --arg n "$workflow" '{workflow_name: $n}')

run_id=$(curl -sf -X POST "$EXEC/runs" -H 'Content-Type: application/json' -d "$body" | jq -r '.id // empty')
if [ -z "$run_id" ]; then
  echo "could not create a run -- is the executor up? (curl $EXEC/health)" >&2
  exit 2
fi
echo "run: $run_id"

start=$(date +%s)
if ! curl -sf -X POST "$EXEC/runs/$run_id/start" >/dev/null; then
  echo "could not start run $run_id -- see: curl -s -X POST $EXEC/runs/$run_id/start" >&2
  exit 2
fi

status=""
while [ $(( $(date +%s) - start )) -lt "$TIMEOUT" ]; do
  status=$(curl -sf "$EXEC/runs/$run_id" | jq -r '.status')
  case "$status" in completed|failed|cancelled) break ;; esac
  sleep 1
done
elapsed=$(( $(date +%s) - start ))

tl=$(curl -sf "$EXEC/runs/$run_id/timeline")
if ! jq -e '.steps | type == "array"' <<<"$tl" >/dev/null 2>&1; then
  echo "could not read the timeline of run $run_id -- see: curl -s $EXEC/runs/$run_id/timeline" >&2
  exit 2
fi
# Every physical execution for this run, from the instruments themselves.
execs=$(instruments | jq -c --arg r "$run_id" '[.[] | select(. != null) | .executions[] | select(.run_id == $r)]')

echo
echo "checks"

# 1. the run finished
if [ "$status" = "completed" ]; then
  ok "run completed (${elapsed}s)"
else
  bad "run did not complete -- status '${status:-unknown}' after ${elapsed}s"
fi

# 2. every step completed
notdone=$(jq -r '[.steps[] | select(.status != "completed") | "\(.name)=\(.status)"] | join(", ")' <<<"$tl")
if [ -z "$notdone" ]; then
  ok "all steps completed"
else
  bad "steps not completed: $notdone"
fi

# 3. no step started before its dependencies finished
violations=$(jq -r '
  (.steps | map({key: .name, value: .}) | from_entries) as $by
  | [ .steps[]
      | . as $s
      | select($s.start_offset_ms != null)
      | $s.depends_on[]?
      | . as $d
      | select(($by[$d].end_offset_ms // null) == null
               or ($by[$d].end_offset_ms > $s.start_offset_ms))
      | "\($s.name) started before \($d) finished" ]
  | join("; ")' <<<"$tl")
started=$(jq '[.steps[] | select(.start_offset_ms != null)] | length' <<<"$tl")
if [ "${started:-0}" -eq 0 ]; then
  note "dependency order: nothing ran, nothing to check"
elif [ -z "$violations" ]; then
  ok "dependency order respected"
else
  bad "$violations"
fi

# 4. independent work actually overlapped
overlaps=$(jq '
  [ .steps as $all
    | range(0; ($all|length)) as $i
    | range(($i+1); ($all|length)) as $j
    | $all[$i] as $a | $all[$j] as $b
    | select($a.device_id != $b.device_id)
    | select($a.start_offset_ms != null and $b.start_offset_ms != null)
    | select($a.end_offset_ms != null and $b.end_offset_ms != null)
    | select($a.start_offset_ms < $b.end_offset_ms and $b.start_offset_ms < $a.end_offset_ms)
    | 1 ] | length' <<<"$tl")
if [ "${overlaps:-0}" -gt 0 ]; then
  ok "independent steps ran at the same time ($overlaps overlapping pair(s))"
else
  bad "nothing ran in parallel"
fi

# 5. no step ran more than once on its instrument
total=$(jq 'length' <<<"$execs")
dupes=$(jq -r 'group_by(.step_name) | map(select(length > 1) | "\(.[0].step_name) x\(length)") | join(", ")' <<<"$execs")
if [ "${total:-0}" -eq 0 ]; then
  bad "no instrument ran anything for this run"
elif [ -z "$dupes" ]; then
  ok "no step ran more than once on its instrument"
else
  bad "steps that ran more than once: $dupes"
fi

# 6. every step the record calls completed, completed on its instrument
unbacked=$(jq -r --argjson ex "$execs" '
  [ .steps[] | select(.status == "completed") | .name as $n
    | select([$ex[] | select(.step_name == $n and .outcome == "completed")] | length == 0)
    | $n ] | join(", ")' <<<"$tl")
completed_steps=$(jq '[.steps[] | select(.status == "completed")] | length' <<<"$tl")
if [ "${completed_steps:-0}" -eq 0 ]; then
  note "record vs instruments: nothing completed, nothing to check"
elif [ -z "$unbacked" ]; then
  ok "every completed step completed on its instrument"
else
  bad "the record says completed, the instrument does not: $unbacked"
fi

# 7. the plate reader's data is stored, and it is the data the instrument produced
reads=$(jq -c '[.steps[] | select(.device_id == "plate-reader-1")]' <<<"$tl")
problems=""
while IFS= read -r row; do
  name=$(jq -r '.name' <<<"$row")
  ref=$(jq -r '.data_ref // empty' <<<"$row")
  want=$(jq -r --arg n "$name" '[.[] | select(.step_name == $n and .outcome == "completed") | .data_sha256] | last // empty' <<<"$execs")
  if [ -z "$ref" ]; then problems="$problems $name:no-data_ref"; continue; fi
  case "$ref" in s3://*) ;; *) problems="$problems $name:data_ref-not-s3://"; continue ;; esac
  path=${ref#s3://}
  got=$(curl -sf --max-time 5 "$S3/$path" | sha256)
  if [ "$got" = "$(printf '' | sha256)" ]; then problems="$problems $name:object-missing"
  elif [ "$got" != "$want" ]; then problems="$problems $name:hash-mismatch"; fi
done < <(jq -c '.[]' <<<"$reads")
if [ "$(jq 'length' <<<"$reads")" -eq 0 ]; then
  note "stored data: this workflow has no plate reader step"
elif [ -z "$problems" ]; then
  ok "plate reader data stored and matches what the instrument measured"
else
  bad "plate reader data:$problems"
fi

echo
echo "for information"
note "total run time: ${elapsed}s"
note "physical executions for this run: $total"

echo
if [ "$fail" -eq 0 ] && [ "$pass" -eq 7 ]; then
  echo "All 7 checks passed."
elif [ "$fail" -eq 0 ]; then
  echo "$pass/7 checks passed, none failed -- the rest could not be checked."
else
  echo "$pass/7 checks passed, $fail failed."
fi
echo "Timeline: curl -s $EXEC/runs/$run_id/timeline | jq"
[ "$fail" -eq 0 ] && [ "$pass" -eq 7 ]
