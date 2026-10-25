# Lab automation challenge: working conventions

The challenge is in `README.md`. This file is how we work on it.

## The invariant

**The run record must be true.** What the API reports for a run and its steps,
and the data in object storage, must match what physically happened. The
instruments' ledgers (`curl -s localhost:710{1,2,3}/status`) are the ground
truth. "The API says completed" is a claim, not evidence.

## Fixed, do not change

- `services/driver/instrument/instrument.go`
- Signatures: `Executor` methods (`NewExecutor`, `Start`, `Close`, `CreateRun`,
  `StartRun`, `CancelRun`, `GetRun`, `ListRuns`) and `Serve(ctx, inst, nc, st)`
- `RunView` / `StepView` JSON names and status values; `data_ref` as `s3://bucket/key`
- HTTP API paths and responses in `api.go`

## Roles

- The user owns every design decision and reviews every PR. Claude writes the code,
  proposes options, and challenges assumptions, including the user's and its own.
- Disagreeing is part of the job. "This doesn't earn its place" is an acceptable answer.
- Label claims as **measured** (we ran it), **inferred** (from reading code), or
  **guess**. Never present a guess as a fact.
- The goal includes the user's understanding. Explain how a change works, why it
  was chosen, and what could go wrong. The user must be able to defend it alone.

## Workflow

- `/decide <question>` for a design fork. Recorded in `docs/decisions.md`.
- `/slice <behaviour>` for one small change, built from a failing check, and opened as one PR.
- `/break <fault>` for a chaos experiment. Recorded in `docs/chaos-log.md`.

Git:
- `main` holds the original challenge. `dev` is the integration branch.
- Each slice gets a branch `slice/<short-name>` off `dev` and a PR into `dev`.
  One reviewable idea per PR. At the end, one PR `dev` → `main`.
- Commit messages say why, not only what.
- Before the PR is opened: run `codex review --base dev` once, then bring Claude's
  own findings and Codex's to the user. Don't apply Codex fixes without asking.

## Review checklist (every change that touches messaging or state)

For each message or state transition, answer:
1. What if it arrives **twice**? **Late** (after a retry or a cancel)? **Never**?
2. What if the process **restarts** between two steps of this code path?
3. Who **acknowledges** it, and is the ack after the effect is durable?
4. Can a status move **backwards** or skip a state? Is the update guarded?
5. Where does each **timestamp** come from? (Prefer the instrument's ledger.)
6. Could the record say something the ledger contradicts?

## Commands

```bash
docker compose up --build -d                       # whole stack
docker compose up --build -d executor liquid-handler-1 incubator-1 plate-reader-1
docker compose down -v                             # after db/init.sql changes
go test ./...                                      # unit tests
./scripts/acceptance.sh                            # happy-path floor
./scripts/chaos.sh random --seed 7 --for 60        # see README "Breaking things"
```

## Bookkeeping for the submission

- `docs/decisions.md` and `docs/chaos-log.md` are written as we go. They feed `NOTES.md`.
- Log session start and end times in `docs/decisions.md`.
- AI conversations are exported to `ai/`.
