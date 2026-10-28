# Concerns

Everything we noticed along the way that didn't have to be fixed at once: limits, risks,
assumptions and loose ends. Anything that would have made the record false was fixed in the
change that found it, so **none of the open items below makes the record false.** They are
about runs that never end, views that lag, operations, and scale.

This page groups the concerns by status and theme, as of 2026-10-27. The working log in
[`../forai/concerns.md`](../forai/concerns.md) keeps each concern's original wording and
review notes. Numbers (K1–K62) are the same in both. For the terms (ledger, attempt, claim,
phase), see [decisions.md](decisions.md#terms-used-on-this-page).

| Status | Count | Meaning |
| --- | --- | --- |
| [Open](#open) | 29 | A real remaining limit. |
| [Accepted](#accepted) | 9 | True, but left on purpose: by design, out of scope, or an assumption. |
| [Resolved](#resolved) | 23 | Fixed; the fix is named. |
| Duplicate | 1 | K52 is the same as K5. |

**The ones that matter most:** K39 and K6 (a run can stay `running` for ever), K62 (an
executor network blip delays runs by 30s), K59 (REST can show a run and its steps out of
step for one poll), K58 (a retried `createRun` creates two runs) and K5 (the acceptance
script fails on retried runs).

---

## Open

### A run can stay `running` for ever

The record is true in each case, because the outcome really is unknown. But nothing ends
the run.

- **K39. A lost read whose re-reads also fail.** When a read's data is lost, the plate is
  read again (D4). If every re-read also fails or loses its data, the read stays
  unresolved, and its run `running`, until the original object appears in storage. The
  planned fix, an operator mutation `abandonStep` that marks the step `failed` ("abandoned
  by operator; a late upload may still appear"), was not built.
- **K6. A driver that never comes back.** A step claimed by a driver that never returns
  stays `running`: we can't know whether the job ran. The fix is the same kind of operator
  action, "declare lost". Not built.
- **K31. A stored object that disappears before it's verified.** If the driver reports
  `stored` but the object is missing or unreadable, the executor checks again every 2s,
  for ever. The step stays `running`, which is true.

### A view that lags, or is incomplete

- **K62. An executor network blip costs 30s.** When the executor is cut off from NATS,
  events that JetStream had just handed to it are lost in transit, and redelivered only
  after the 30s ack timeout, however short the cut. Measured in C4: an 8s cut delayed the
  run by 30s, and the record said `running` for steps that had ended. Fix: lower the ack
  timeout to about 10s (twice the time an event takes to handle). A duplicate delivery is
  harmless.
- **K59. REST's `GetRun` is not one snapshot.** It reads the run and its steps in two
  queries. For one poll, at the moment a run finishes, it can show the run `running` with
  every step ended, or the reverse. GraphQL reads one snapshot. Fix: wrap `GetRun` in the
  same transaction; its response doesn't change.
- **K27. Why a step never ran.** In a failed run, the steps after the failure stay
  `pending`. That is true (they were never sent), but a reader may expect `cancelled`.
  GraphQL's `phase` and empty `attempts` show the step was never sent, but nothing says
  why.
- **K34. A verified object deleted later.** A read verified once stays `completed` with its
  `data_ref` (D2), even if the object is deleted afterwards. Fix: re-check completed reads
  periodically and show a warning, as D2 already does for a mismatch.
- **K46. A failed step can become unresolved again.** If a conflicting outcome arrives
  for an attempt that has already ended (a duplicate or corrupted report), the step moves
  from `failed` back to `unresolved`, but its run, already `failed`, isn't reopened.
  Needs a corrupted report to happen.
- **K61. Step and attempt times differ below a microsecond.** A step's times are stored by
  Postgres to the microsecond; its attempts' times are the ledger's, to the nanosecond. A
  tool comparing them exactly will see a difference. Fix: round attempt times in GraphQL,
  or say so in the schema.

### Faults and clients

- **K5. The acceptance script fails on retried runs.** A retried step is two jobs in the
  ledger, so acceptance check 5 ("no step ran more than once") fails whenever faults
  cause a retry (seen in C3 and C4). The script is meant for the happy path, and the
  retries are honest, so it is left unchanged and explained in NOTES.md. (K52 is the same
  concern.)
- **K51. Retries have no backoff.** A retry is sent at once. A device that fails every
  time uses its 3 attempts within seconds, so a fault lasting longer than that ends the
  step. Add a backoff per attempt if it matters.
- **K58. `createRun` is not idempotent.** A client that retries after losing the response
  creates a second run. Fix: an optional client request id, unique per run.

### Configuration

- **K9. Data-producing devices are recognised by name.** A step produces data if its device
  name starts with `plate-reader` (`store.go`, `producesData`). A new kind of device that
  produces data would complete without its data being checked. Fix: drivers announce
  whether they produce data.
- **K13. Device names are not validated.** They go into NATS subjects as they are. A `.`,
  `*` or `>` in a device name in `workflows.yaml` would break or widen a subject. Fix:
  validate names when workflows are loaded.

### Operations and testing

- **K22. The event stream keeps everything.** `LAB_EVENTS` has no age or size limit, so it
  grows for ever. Set a retention once we know how far back replay needs.
- **K23. Rejected events are only logged.** A malformed event is logged and dropped, with
  no dead-letter queue to inspect it later.
- **K36. How late a dead process's upload can land is untested.** The driver's grace
  period before reporting `data_lost` (2 × the 5s storage timeout) only avoids a needless
  report (D3). Measuring it would need a storage-side test.
- **K38. A replay property shown only by the code.** Replay uses the ledger as it was
  before the driver started listening, so a read that starts during replay is never called
  lost. No test would catch a change to that: the existing test passes either way, because
  replay finishes before any command arrives.

### Scale

Each of these is fine with three devices and a handful of runs, and each has an obvious fix
if it starts to matter.

| # | What grows | Fix if it shows |
| --- | --- | --- |
| K20 | The driver copies the whole ledger on every command, and every 50ms while a job runs. | Look jobs up by id. |
| K24 | Claims are handled one at a time, so a slow database delays every claim. | A worker pool. |
| K28 | Every 500ms the executor scans every running run, one query per run. | One joined query, or `LISTEN/NOTIFY`. |
| K29 | Runs that share a device just retry on `busy`, with no fairness between them. | A queue per device. |
| K33 | Each redelivery of `stored` adds another storage check to the step. | Drop checks with the same hash. |
| K35, K37 | Each driver restart replays its whole ledger, and the executor reads every stored read back again. | Replay only jobs after a checkpoint. |
| K40 | Every lost read's object is checked every 10s, for ever. | Back off per read. |
| K49 | The lost-read check decodes every running step every 10s. | Filter in SQL. |
| K50 | The dispatch loop locks every `ended` or `unresolved` step every 500ms, even when nothing can change. | Skip steps until a new fact arrives. |
| K60 | GraphQL `runs` returns every run, with all steps and attempts. | Pagination (`first`/`after`). |

---

## Accepted

### Assumptions

- **K55. One driver per instrument, and a ledger that survives restarts.** "A resend
  never runs a job twice" depends on both. A wiped ledger directory, or a second driver for
  the same instrument, would let a resend run a job again. Not reachable in this stack.
- **K56. One executor.** The withdrawn claim (D5) assumes a second claim for a job means
  the first was never acted on. With two executors, a driver's first claim could stall in
  one while its retry is granted by the other and the job runs. If a cancel then commits
  before the stalled claim, that claim would look like a re-claim and withdraw an attempt
  that is running. With replicas, drivers would number their claims so a stale one is
  recognised.
- **K16. The instrument's ledger write succeeds.** If the instrument's end-of-job ledger
  write fails, it reports `completed` and we record it, but after a restart its ledger says
  `hung` (`instrument.go:241`). The record would then disagree with the ledger. It needs a
  disk error, and we can't see it from outside the instrument.

### By design

- **K30. Storage down holds the plate reader.** The read's bytes exist only in the driver's
  memory, so the driver keeps the device busy until they are stored. A long outage stops
  all reads. Measured in C2 and C4: the read waited and completed with the right hash. The
  cost is the outage plus about 16s for SeaweedFS to restart. A spool to disk would remove
  the hold.
- **K43. A failure that isn't retried after a crash.** If the driver dies between the
  instrument recording a failure and `Run` returning, replay reports the failure without
  the retryable flag, so it isn't retried. The window is microseconds, and it errs on not
  repeating.
- **K14. No rolling upgrades.** A driver speaking a newer protocol version against an older
  executor has every message rejected. That is safe, but not zero-downtime. Out of scope.

### Repository and tests

- **K8. The C1 probe isn't in the repository.** The NATS partition experiment used a
  local-only script, so C1 can't be fully reproduced from the repo.
- **K7. Local packages under `tmp/`.** `go vet ./...` also checks the untracked `tmp/`
  packages in this checkout. A fresh clone doesn't have them.
- **K57. A surviving mutant.** Judging the latest attempt instead of the one that ran,
  after a cancel, gives the same answer: a withdrawn attempt is always a retry the policy
  allowed. It would differ only if the retry settings changed across a restart. Noted so
  a reviewer doesn't take it for a missing test.

---

## Resolved

| # | Problem | Fixed by |
| --- | --- | --- |
| K1 | REST's `error` mixes why a step is unresolved, why it failed, and a warning on a completed read. | GraphQL shows each storage read-back with `matches`, and `verified` (D6). REST is unchanged. |
| K2 | A step kept only one attempt; earlier evidence was dropped. | Every attempt is kept (PRs #14, #15). |
| K3 | A failure with no instrument error gave `error: null`. | `error` always ends with the reason the policy gave up (D4). |
| K4 | A slow upload could be reported lost. | Only a restarted driver reports `data_lost`, after a grace period (PR #11). |
| K10 | The stored step had no version, so renaming a field would lose data. | The stored step carries a version; an unknown one is refused (PR #4). |
| K11 | Acking an event before it was saved, or answering a claim from a stale read, would lose facts. | Events are acked after the commit; claims are answered from the saved step (PR #8). |
| K12 | Locking several steps in any order could deadlock. | The cancel locks steps in position order (PR #19). |
| K15 | A driver could report for a device that isn't its own. | Events must match their subject's device and the step's assigned device (PR #8). |
| K17 | The driver dropped the plate reader's data. | The driver uploads it, then reports `stored` (PR #10). |
| K18 | A driver restart before publishing `finished` lost the outcome. | Replay reports every ledger outcome on start (PR #11). |
| K19 | A claim granted, but the instrument busy with another job: nothing ran, and the step stayed claimed. | The same job is resent while claimed with no evidence (PR #18). |
| K21 | `stored` events were redelivered every 30s before anything handled them. | Handled since PR #10. |
| K25 | A hung step, or one claimed by a driver that died, kept its run `running` for ever. | Hangs end by the retry policy (D4); the resend finds a job that never started (PR #18). |
| K26 | A late upload completed a read after its run had finished `failed`. | A lost read now stays unresolved, so the run never finishes early (D3). |
| K32 | A driver restart between a read and its upload lost the bytes, and the read stayed `running`. | Replay checks storage and reports `stored` or `data_lost` (PR #11). |
| K41 | Runs finished `failed` under the old rule weren't reopened. | Obsolete: the local database was reset. |
| K42 | No rule for a cancelled run's final status. | D5's rule (PR #19). |
| K44 | A race in the driver was caught only by `go test -race`. | `-race` is part of every check. |
| K45 | Events for an earlier attempt were dropped. | Events find their step from the job id (PR #15). |
| K47 | Every command said attempt 1. | The attempt comes from the job id (PR #16). |
| K48 | A plate-reader hang wasn't retried. | `REPEAT_SAFE` defaults to the plate reader (PR #16). |
| K53 | After a retry, REST and `/timeline` show only the deciding attempt, hiding when the device was busy. | GraphQL lists every attempt with its ledger times (PR #20). REST is unchanged. |
| K54 | A cancel after a claim, for a job that never started, didn't stop it. | A re-claim after a cancel is withdrawn (PR #19). |
