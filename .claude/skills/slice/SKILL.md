---
name: slice
description: Build one small, reviewable change for the lab automation challenge, starting from a failing check, explained to the user, reviewed by Codex once, and opened as one PR into dev. Use when the user says /slice or asks to implement the next piece.
---

# /slice <behaviour>

One slice = one behaviour, one check that proves it, one PR. If it needs more
than about 150 changed lines of non-test code, or two unrelated ideas, split it
and say so before starting.

## Steps

1. **Frame it.** Before any code, tell the user in a few lines:
   - the behaviour in one sentence, as an observer would see it (API, ledger, S3);
   - the check that proves it: a `go test`, `acceptance.sh`, or a scripted run;
   - which design decisions it depends on (link `docs/decisions.md` entries).
     If one is missing, stop and run `/decide` first.
   Wait for the user's OK, unless the slice is mechanical.

2. **Branch.** `git switch dev && git pull && git switch -c slice/<short-name>`.

3. **Failing check first.** Write the test or check and run it. Show that it fails
   for the right reason. If no automated check is practical, write the exact manual
   command and its expected output, and say why automation was skipped.

4. **Smallest change that passes.** No speculative abstractions. No hardening beyond
   this slice's behaviour. Note anything deferred.

5. **Verify.** `go build ./... && go vet ./... && go test ./...`, plus the slice's
   own check. If the slice touches the running system, rebuild the stack and run
   `./scripts/acceptance.sh`. Report results as measured, with output.

6. **Self-review** against the checklist in `CLAUDE.md`. Write down each
   "what could go wrong" you find, even ones you won't fix in this slice.

7. **Codex review, once.** Commit, then run `./scripts/codex-review.sh` (read-only,
   diffs against `dev`, prints only Codex's verdict; takes about a minute). Triage each finding: fix it, defer it, or reject it with a reason. Do not apply
   fixes silently. Bring only blockers to the user; add non-blocking findings (Claude's
   and Codex's) to `docs/concerns.md`.

8. **Explain it to the user.** This is the review conversation, so write it to teach:
   - **How it works:** the path a message or request takes, file:line references.
   - **Why this way:** the choice made and the alternative not taken.
   - **What could go wrong:** failure modes, which ones this slice handles, which it
     doesn't yet, and how we'd notice.
   - **Evidence:** test and check output.
   - **Codex findings:** each one with its triage.
   - One or two questions the user should be able to answer about this change.
     They are prompts for understanding, not a quiz.

9. **PR after the user agrees.** Push and `gh pr create --base dev` with a body that
   covers what, why, what could go wrong, and evidence, ending with the attribution
   line from the system reminder. Add a `docs/decisions.md` entry if the slice settled
   anything new.
