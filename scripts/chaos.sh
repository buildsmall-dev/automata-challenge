#!/usr/bin/env bash
# Breaks things on purpose.
#
#   ./scripts/chaos.sh kill executor                 # kill the executor process, then start it again
#   ./scripts/chaos.sh disconnect <service> <secs>   # cut a service off the network for a while
#   ./scripts/chaos.sh storage-down <secs>           # stop object storage for a while
#   ./scripts/chaos.sh random [--seed N] [--for SECS]
#
# disconnect takes: executor, liquid-handler-1, incubator-1, plate-reader-1.
# A disconnected driver's instrument carries on with whatever it was doing.
#
# random runs one fault at a time, chosen from the three above, until SECS have
# passed (default 60). The same seed gives the same faults in the same order at
# the same times.
#
# Instrument faults are separate: set LH_/INC_/PR_FAIL_PCT and LH_/INC_/PR_HANG_PCT
# and restart that driver, e.g.  INC_HANG_PCT=20 docker compose up -d incubator-1
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.."

DRIVERS=(liquid-handler-1 incubator-1 plate-reader-1)

say() { printf '%s chaos: %s\n' "$(date +%H:%M:%S)" "$*"; }

container() {
  local id
  id=$(docker compose ps -q "$1")
  if [ -z "$id" ]; then echo "chaos: service $1 is not running" >&2; exit 1; fi
  echo "$id"
}

# Whatever a fault must undo. Run on any exit, including Ctrl-C, so an
# interrupted fault never leaves a service cut off or stopped.
restore=""
undo() {
  if [ -n "$restore" ]; then
    eval "$restore"
    restore=""
  fi
}
trap undo EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# sleep in the background and wait for it: bash runs a trap only between
# commands, and wait, unlike sleep, returns as soon as a signal arrives.
pause() {
  sleep "$1" &
  wait $!
}

whole_number() {  # whole_number <name> <value>
  case "$2" in
    ''|*[!0-9]*) echo "chaos: $1 must be a whole number, got '$2'" >&2; exit 2 ;;
  esac
}

network_of() {
  docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$1"
}

do_kill() {
  [ "$1" = "executor" ] || { echo "chaos: kill takes 'executor' only" >&2; exit 2; }
  local id
  id=$(container executor)
  say "kill executor"
  restore="docker start '$id' >/dev/null; say 'executor started again'"
  docker kill "$id" >/dev/null
  undo
}

do_disconnect() {
  local svc=$1 secs=$2 id net ip
  whole_number secs "$secs"
  case "$svc" in
    executor|liquid-handler-1|incubator-1|plate-reader-1) ;;
    *) echo "chaos: disconnect takes executor or a driver, got '$svc'" >&2; exit 2 ;;
  esac
  id=$(container "$svc")
  net=$(network_of "$id")
  ip=$(docker compose config --format json | jq -r --arg s "$svc" '.services[$s].networks["lab-network"].ipv4_address')
  say "disconnect $svc for ${secs}s"
  # Same address and the same name as before: a partition, not a new machine.
  restore="docker network connect --ip '$ip' --alias '$svc' '$net' '$id'; say 'reconnect $svc'"
  docker network disconnect "$net" "$id"
  pause "$secs"
  undo
}

do_storage_down() {
  local secs=$1 id
  whole_number secs "$secs"
  id=$(container storage)
  say "storage down for ${secs}s"
  restore="docker start '$id' >/dev/null; say 'storage up'"
  docker kill "$id" >/dev/null
  pause "$secs"
  undo
}

# A small linear congruential generator. Bash's own $RANDOM differs between
# versions, so it cannot promise the same schedule on every machine.
rng_state=0
next() {  # next <n>: sets $r to a number in [0, n)
  rng_state=$(( (rng_state * 1103515245 + 12345) % 2147483648 ))
  r=$(( (rng_state / 65536) % $1 ))
}

do_random() {
  local seed=1 duration=60
  while [ $# -gt 0 ]; do
    case "$1" in
      --seed) seed=$2; shift 2 ;;
      --for)  duration=$2; shift 2 ;;
      *) echo "chaos: unknown option $1" >&2; exit 2 ;;
    esac
  done
  whole_number --seed "$seed"
  whole_number --for "$duration"
  # Keep the state inside the generator's range, so large seeds work too.
  rng_state=$(( 10#$seed % 2147483648 ))
  say "random faults for ${duration}s, seed $seed"
  local start gap secs
  start=$(date +%s)
  while :; do
    next 8; gap=$(( 3 + r ))
    next 6; secs=$(( 2 + r ))
    if [ $(( $(date +%s) - start + gap )) -ge "$duration" ]; then break; fi
    pause "$gap"
    next 4
    case "$r" in
      0) do_kill executor ;;
      1) do_disconnect executor "$secs" ;;
      2) next 3; do_disconnect "${DRIVERS[$r]}" "$secs" ;;
      3) do_storage_down "$secs" ;;
    esac
  done
  say "random faults finished"
}

cmd=${1:-}
[ $# -gt 0 ] && shift
case "$cmd" in
  kill)         [ $# -eq 1 ] || { echo "usage: chaos.sh kill executor" >&2; exit 2; }; do_kill "$1" ;;
  disconnect)   [ $# -eq 2 ] || { echo "usage: chaos.sh disconnect <service> <secs>" >&2; exit 2; }; do_disconnect "$1" "$2" ;;
  storage-down) [ $# -eq 1 ] || { echo "usage: chaos.sh storage-down <secs>" >&2; exit 2; }; do_storage_down "$1" ;;
  random)       do_random "$@" ;;
  *) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
