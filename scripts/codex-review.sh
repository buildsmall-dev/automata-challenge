#!/usr/bin/env bash
# One independent Codex review of the current branch against a base (default dev).
#
#   ./scripts/codex-review.sh [base]
#
# Read-only: Codex can run git and read files, but cannot change anything.
# `codex review --base` takes no custom instructions, so this uses `codex exec`.
set -euo pipefail

base=${1:-dev}
cd "$(dirname "$0")/.."
log=$(mktemp -t codex-review.XXXXXX)
final=$(mktemp -t codex-review-final.XXXXXX)
trap 'rm -f "$final"' EXIT

codex exec -s read-only --ephemeral -o "$final" "You are reviewing a change in this repository. Run \`git diff $base...HEAD\` and \`git log $base..HEAD\` to see it. Read CLAUDE.md and README.md for context.

The system drives lab instruments over NATS. Its one invariant: the run record (API and stored data) must be true, so it must match the instrument ledgers and the stored plate reader data.

Report only real problems, most severe first, with file:line and a concrete failure scenario. Focus on:
- duplicate, late, reordered, or lost messages; process restarts between two steps
- acknowledgement before the effect is durable
- status transitions that can go backwards or be skipped
- the record claiming something the ledger or the stored data contradicts
- anything that contradicts CLAUDE.md or the README's fixed shapes
Ignore style and naming. If you find nothing, say so." < /dev/null > "$log" 2>&1 \
  || { echo "codex failed; transcript: $log" >&2; tail -20 "$log" >&2; exit 1; }

cat "$final"
echo
echo "(full transcript: $log)"
