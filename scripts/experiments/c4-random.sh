#!/usr/bin/env bash
# scripts/experiments/c4-random.sh <seed> <out>: chaos.sh random for 60s, with PCR runs started back to back
# until it ends; then wait for every run to settle and check them all.
set -uo pipefail
cd /Users/yudu/Dev/Automata/distributed-systems-interview
seed=$1; out=$2; : > $out.runs
./scripts/chaos.sh random --seed $seed --for ${FOR:-60} > $out.chaos 2>&1 & chaos=$!
api() { curl -s --max-time 3 "$@"; }
while kill -0 $chaos 2>/dev/null; do
  id=$(api -XPOST localhost:5002/runs -H 'content-type: application/json' -d '{}' | jq -r .id 2>/dev/null)
  case $id in run-*) ;; *) sleep 1; continue ;; esac
  until api -XPOST localhost:5002/runs/$id/start >/dev/null; do sleep 1; done
  echo $id >> $out.runs
  for i in $(seq 90); do s=$(api localhost:5002/runs/$id | jq -r .status 2>/dev/null); case $s in completed|failed|cancelled) break;; esac; sleep 1; done
done
for id in $(cat $out.runs); do
  for i in $(seq 150); do s=$(api localhost:5002/runs/$id | jq -r .status 2>/dev/null); case $s in completed|failed|cancelled) break;; esac; sleep 1; done
done
cat $out.chaos
./scripts/check-run.py $(cat $out.runs)
