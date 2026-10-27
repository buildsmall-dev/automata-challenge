#!/usr/bin/env bash
# One C4 case: start a PCR run, inject the fault at an offset, wait, check.
#   scripts/experiments/c4-fault.sh <case>    cases: a b c d e f g h
set -uo pipefail
cd /Users/yudu/Dev/Automata/distributed-systems-interview
crash() { docker compose kill "$1" >/dev/null 2>&1; sleep "$2"; docker compose start "$1" >/dev/null 2>&1; echo "$(date +%T) $1 killed, started after $2s"; }
case $1 in
  a) at=3;   fault() { ./scripts/chaos.sh kill executor; } ;;
  b) at=9;   fault() { ./scripts/chaos.sh kill executor; } ;;
  c) at=9;   fault() { crash plate-reader-1 3; } ;;
  d) at=7;   fault() { crash liquid-handler-1 3; } ;;
  e) at=1;   fault() { crash incubator-1 5; } ;;
  f) at=3;   fault() { ./scripts/chaos.sh disconnect executor 8; } ;;
  g) at=8.5; fault() { ./scripts/chaos.sh disconnect plate-reader-1 10; } ;;
  h) at=7;   fault() { ./scripts/chaos.sh storage-down 15; } ;;
esac
id=$(curl -s -XPOST localhost:5002/runs -H 'content-type: application/json' -d '{}' | jq -r .id)
curl -s -XPOST localhost:5002/runs/$id/start >/dev/null; t0=$(date +%s)
echo "$(date +%T) case $1: run $id started; fault at ${at}s"
sleep $at; fault
for i in $(seq 120); do
  s=$(curl -s --max-time 2 localhost:5002/runs/$id | jq -r .status 2>/dev/null)
  case $s in completed|failed|cancelled) break ;; esac; sleep 1
done
echo "$(date +%T) run $id: $s, $(( $(date +%s) - t0 ))s after start"
./scripts/check-run.py $id
curl -s localhost:5002/runs/$id/timeline | jq -c '.steps[] | {name, status, start_offset_ms, end_offset_ms}'
