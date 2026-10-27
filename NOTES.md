# NOTES

The rule I worked to is the one in the README: **the run record must be true.** Every claim
the record makes needs its own evidence:

- **How a step ran and when:** the instrument's ledger.
- **A read's data:** an object in storage whose sha256 matches the ledger.
- **Without evidence:** the record says `running` while the outcome can still be learned, and
  `failed` with the reason once it can't. It never says `completed` without evidence.

Where to find the detail behind this note:

| | |
| --- | --- |
| `docs/forengineer/decisions.md` | D1–D6: each design decision, its alternatives, and what it rests on |
| `docs/forengineer/chaos-log.md` | C1–C4: each experiment, predicted before it ran, then observed |
| `docs/forengineer/concerns.md` | K1–K62: everything noticed along the way, with a status (resolved, accepted, open) |
| `docs/html/` | Explainers written for the two biggest decisions, D1 and D4 |
| `docs/forai/` | The same three records as written during the work, with their history and review notes |
| `scripts/check-run.py` | Compares a run's record with all three ledgers and with storage |

The work went in through 22 PRs into `dev`. Each one carries a check that failed first, its
evidence, and a read-only Codex review.

Claims below are labelled **measured** (ran it), **inferred** (read the code) or **guess** where
it matters.

---

## 1. Topology

```
             Postgres (runs, steps.lifecycle)  ◄── the only decider
                          ▲
                          │ one transaction per fact, under the step's row lock
 ┌────────────────────────┴───────────────┐
 │ executor                                │
 │  dispatch loop (500ms + poke)           │                     ┌──────────────┐
 │  claim handler   event consumer         │ ◄── reads back ──── │ S3 storage   │
 └──┬──────────────────▲──────────▲────────┘                     └──────▲───────┘
    │ (1) core req     │ (2) core │ (3) JetStream                       │ (4) PUT
    │ lab.<dev>.cmd.run│ req      │ stream LAB_EVENTS                   │ runs/<run>/<step>/<n>.csv
    │ accepted|busy|   │ lab.     │ lab.<dev>.evt.{started,finished,    │
    │ known            │ executor │                stored,data_lost}    │
    ▼                  │ .claim   │ durable pull consumer "executor"    │
 ┌─────────────────────┴──────────┴─────────────────────────────────────┴──┐
 │ driver (one per instrument)                                              │
 │  accept → claim → inst.Run → read the ledger → publish → upload          │
 └──────────────────────────────┬───────────────────────────────────────────┘
                                │ in process
                       instrument + its ledger (on disk, survives restarts)
```

### What goes over the wire

| # | Subject | Kind | From → to | Reply / ack |
| --- | --- | --- | --- | --- |
| 1 | `lab.<device>.cmd.run` `{v, job_id, run_id, step, attempt}` | core request/reply | executor → driver | `accepted`, `busy` or `known`. No reply means unknown |
| 2 | `lab.executor.claim` `{v, job_id, device}` | core request/reply, queue group `executor` | driver → executor | `go` or `abort`. No reply if the database fails |
| 3 | `lab.<device>.evt.<kind>`, kind = `started`, `finished`, `stored` or `data_lost` | JetStream stream `LAB_EVENTS` (file storage), `Nats-Msg-Id` = `<job_id>:<kind>` | driver → executor | The driver retries until it gets a PubAck. The executor acks only after its database commit |
| 4 | `runs/<run>/<step>/<attempt>.csv` | S3 PUT, then a `stored` event | driver → storage | The executor reads the object back and hashes it |

`job_id` is `<run>/<step>/<attempt>`. A job is one attempt at one step, and it is the unit of
idempotency everywhere.

### One step, in order

1. **Dispatch.** The dispatch loop picks the steps `NextSteps` says are ready. It records a
   `Dispatched` fact, then sends the command.
   - **`busy` or no answer:** asks again after 500ms.
   - **`accepted` or `known`:** asks again after 3s, until there is evidence the job started.
   - **A timeout:** means *unknown*. The command is resent with the same `job_id`, never
     as a new attempt.
2. **Claim.** Before `inst.Run`, the driver asks `lab.executor.claim`.
   - **The executor's side:** it applies `Claimed` under the step's row lock, the same lock a
     cancel takes, so exactly one of claim and cancel wins.
   - **`go`:** the driver runs the job.
   - **`abort` or no answer:** the job doesn't run. On no answer the driver asks again.
3. **Run and report.** The driver reads how the job went from the **ledger**, not from `Run`'s
   return value. It publishes `started` and `finished` with the ledger's times and outcome,
   and forwards the instrument's `retryable` flag on a failure.
4. **Data.** For a read, the driver uploads the bytes, retrying until storage takes them. It
   keeps the device busy meanwhile, because the bytes exist only in its memory. Then it
   publishes `stored`.
5. **Verify.** The executor does not take `stored` on trust. It reads the object at the key it
   derives itself and hashes it.
   - **Hash matches the ledger's sha256:** the read is completed, and only then does it get a
     `data_ref`.
6. **Apply.** Every event becomes one fact, applied idempotently in one transaction. Then the
   event is acked. Messages that can't apply are handled like this:
   - **Malformed, or from a device that doesn't match its subject:** Term'd.
   - **For a job no step has:** acked and logged.
   - **Database error:** Nak'd, and retried after 1s.
   - **An object not readable yet:** Nak'd, and retried after 2s.

### Recovery paths

- **Driver restart (R1):** before taking commands, the driver replays its whole ledger as
  `started` and `finished` events.
  - **A job cut off by the restart:** the instrument closes it as `hung` (`instrument.go:341`).
  - **A completed read:** the driver asks storage. Present means it publishes `stored`.
    Absent after an upload grace (2 × the storage timeout) means it publishes `data_lost`.
    No answer means it asks again.
- **Claimed, but no evidence (S7):** the same command is resent every 3s. The command itself
  asks the driver what it knows:
  - **`known`:** the job is in flight or in the ledger, so it ran or is running.
  - **`accepted`:** the driver has no record of the job, so it never started. It runs once.
  - **No answer:** the outcome stays unknown, and the command is asked again.
- **Executor restart:** all intent is in Postgres and every event waits in the stream.
  Nothing in executor memory needs recovering.
- **Lost read (D3):** every 10s the executor searches storage for the data of reads
  reported lost.

### Where each piece of state lives

| State | Where | Notes |
| --- | --- | --- |
| Runs, steps, every attempt and every fact about it | Postgres: `runs`, and `steps.lifecycle` (JSONB) | **Source of truth for intent.** `status`, `phase`, the times, `error` and `data_ref` are derived from the lifecycle and written in the same `UPDATE` |
| What physically happened | Each instrument's ledger, on its disk | **Source of truth for physical facts.** Written before work starts, and survives restarts |
| Facts in transit | JetStream `LAB_EVENTS` | Durable, so they wait while the executor is down. Duplicates are dropped by Msg-Id within 2 min, and by idempotent apply after that |
| Plate data | S3, at `runs/<job_id>.csv` | The key is derived from the job, so a retried upload overwrites rather than adds |
| A read's bytes before they're stored | Driver memory | The one place where data can be lost. Covered by replay and D3 |
| Queue of what to run next | Postgres (the step phases) + `NextSteps` | There is no queue in NATS |

**Lifecycle phases:**
- `pending` and `dispatched` show as API `pending`.
- `retrying`, `claimed`, `running`, `storing`, `unresolved` and `ended` show as API `running`.
- `completed`, `failed` and `cancelled` are terminal.

A run finishes only when nothing is in flight. Its `finished_at` is the last ledger time, not
the time the executor noticed.

---

## 2. Why this topology

**One rule shaped all of it:** durable facts, short-lived intentions, and Postgres decides.

### D1. Commands by request/reply, facts by JetStream, claim before run

**Chosen:**
- **Commands** are core request/reply.
- **Facts** go through JetStream.
- **The driver claims** a job, under the same row lock as a cancel, before running it.

**Instead of:**
- *A, request/reply plus executor polling:* simplest. But while the executor is down, facts
  live only in driver memory and the ledger. It also assumes the instrument can always be
  asked, which is true of this simulator and false of most equipment.
- *B, JetStream both ways:* a durable command queue makes stale physical commands possible,
  such as a dispense delivered after a cancel. It also forces AckWait tuning across jobs
  that last from 2s to hours, with 30s hangs.
- *D, shared state in NATS KV:* a second source of truth beside Postgres, without
  transactions.
- *Command expiry or a cancel broadcast, instead of a claim:* both narrow the window for a
  late command, but they race with the cancel, and expiry depends on clocks.

**Why, measured first (C1).** I partitioned a bare NATS client before writing any code, and
my prediction was wrong twice:
- With default pings, nothing was lost but everything was late. That included requests the
  sender had already given up on, which arrived about 20s late.
- 7 of 8 JetStream publishes that reported a timeout had in fact been stored.

So a timeout means *unknown*, never "it didn't happen". That rules out "no reply, so send a
new attempt" and anything that treats a timeout as "not delivered". It's why the design
has:
- the claim;
- resending the same `job_id`;
- Msg-Id plus idempotent apply;
- an explicit `PingInterval` (5s, 2 outstanding).

### Reconcile by resending, not by a query (S7)

- **The case:** a step that is claimed with no evidence. The driver died after its claim, or
  the instrument refused the job.
- **Chosen:** resend the same command.
- **Instead of:** a separate `job` query subject.
- **Why:** the command already asks the driver what it knows of the job. Its idempotence
  (`known` if the job is current, on the instrument, or in the ledger) is what makes the
  resend safe. A query would add a subject and driver code to learn nothing more.
- **Measured live:** a driver killed after its claim was granted but before it started. The
  job ran exactly once after the restart.

### D2. A verified completion is never undone

- **Chosen:** if a later read-back no longer matches the ledger, the step stays `completed`
  and the mismatch is reported.
- **Why:** "completed, data verified at T" stays true for ever. Moving the status back would
  regress it after dependent steps had already run.

### D3. A read reported lost stays open

- **Chosen:** `data_lost` makes the read *unresolved* (API `running`), not failed. The
  executor keeps looking in storage, and a matching object completes the read.
- **Why:** a dead driver's upload can still land later, and nothing bounds when.
- **Found in C2:** the first version failed the read after a grace period, and a late upload
  then contradicted the record.
- **Rejected alternative, making loss final with a create-only write (`If-None-Match: *`):**
  SeaweedFS 3.62 ignores the header. A second conditional PUT returned 200 and overwrote
  the object (measured).

### D4. Retry per cause and per device, bounded

**Chosen:** a new attempt needs a positive claim that repeating is safe, from the source
for its cause:

| Cause | Who says it is safe | Default |
| --- | --- | --- |
| Failure | The instrument's `retryable` flag | Liquid handler: no. Incubator and plate reader: yes |
| Hang | Per-device setting `REPEAT_SAFE` | Plate reader only |
| Data lost | Per-device setting `REREAD_ON_DATA_LOSS` | Plate reader only |

- **Limit:** at most 3 attempts in total (`MAX_ATTEMPTS`).
- **No timers:** a hang is retried only after the instrument itself reports `hung`, so a
  retry is never sent to a frozen instrument.
- **A step is decided by all its attempts:**
  - it is completed once any attempt is verified;
  - it is failed only when every attempt has ended and no more will be made.

**Instead of:**
- *An operator decides every retry:* runs then never end on their own. Under any fault seed
  that is measured, because the fault roll is deterministic.
- *The instrument's flag only:* a hang carries no flag, so this can't decide the hardest
  case.
- *Treating the incubator as repeat-safe:* Triple Assay would then finish under hang faults,
  but by incubating a plate twice for an unknown total time.

**The trade:** with incubator or liquid-handler hangs on, runs fail ("outcome unknown; not
repeated"). That is the honest answer. Which operations are safe to repeat on real equipment
is a **guess**, which is why it's a setting and not code.

### D5. Cancel stops what has not started and keeps the evidence of what has

- **One transaction:** the cancel locks every step of the run in a fixed order.
- **Sent but not claimed:** cancelled. Claim and cancel take the same lock, so exactly one
  wins.
- **Claimed or running:** not stopped. Its late facts are still recorded.
- **After the cancel:** no new attempts.
- **Withdrawn claim (K54):**
  - The case: a claim that was granted, but the driver died before starting.
  - What happens: the restarted driver asks for the claim again. A driver only asks before
    it runs, so the second request proves the job never started. The executor withdraws the
    claim and answers `abort`.
  - Measured live: the ledger has nothing for that job.
- **Run status, rule 1:**
  - `completed` if every step completed;
  - `failed` if a step failed for its own reason;
  - `cancelled` otherwise.
- **Instead of:** "cancelled whenever a cancel was asked for". That would hide a failure that
  happened anyway.
- **Found by Codex:** the first version let a cancel relabel a failure that was final anyway
  as "not retried: cancelled". Now the retry policy decides that, not the cancel.

### D6. GraphQL is a second view of the same rows

See §3.

---

## 3. GraphQL schema

`POST /graphql` on port 5002. The REST API is unchanged. The full SDL, with field
descriptions, is at the top of `services/executor/graphql.go`. In short:

```graphql
type Query {
  run(id: ID!): Run                       # null if unknown
  runs(status: RunStatus): [Run!]!        # newest first
  workflows: [Workflow!]!
}
type Mutation {
  createRun(workflowName: String): Run!
  startRun(id: ID!): Run!
  cancelRun(id: ID!): Run!
}
type Run  { id workflowName status createdAt startedAt finishedAt
            cancelRequested: Boolean!     # may still be running: claimed work isn't stopped
            steps: [Step!]! }
type Step { name device dependsOn status   # status and times exactly as REST
            phase: StepPhase!             # DISPATCHED CLAIMED RUNNING STORING UNRESOLVED ENDED …
            startedAt finishedAt error dataRef
            attempts: [Attempt!]! }       # every attempt, first first
type Attempt { number jobId phase decided claimed
               outcome: Outcome           # COMPLETED FAILED HUNG; null until the ledger says
               startedAt finishedAt       # the ledger's times for this attempt
               instrumentError retryable unresolved
               data: AttemptData }        # reads only
type AttemptData  { ledgerSha256 verified dataLost checks: [StorageCheck!]! }
type StorageCheck { ref sha256 matches at }  # `at` is the executor's clock, not the ledger's
```

Errors carry `extensions.code`: `NOT_FOUND`, `BAD_USER_INPUT`, `WORKFLOWS_INVALID` or
`INTERNAL`. Internal detail goes only to the log, as in REST.

**Why this shape:**

- **It can't contradict REST.**
  - It keeps no state of its own: a step's fields are the same columns REST reads, and
    `attempts` is the lifecycle stored in the same row.
  - A run and all its steps are read in one repeatable-read snapshot.
  - Mutations are the same `Executor` methods.
- **It carries what REST's fixed shape can't:**
  - **Every attempt with its own ledger times.** In C3 and C4c a hung first read held the
    plate reader for 30s (C3) and about 5s (C4c), and REST only shows the attempt that
    decided the step (K53). Measured live: three hung attempts in GraphQL matched three ledger
    executions, while REST's `/timeline` showed only the last.
  - **The data evidence:** the ledger's digest and every storage read-back, with `matches`.
    A corruption warning on a completed read then has its own field, instead of sharing
    `error` with two other meanings (K1).
  - **`phase`:** a scientist can tell "sent, not yet started" (a cancel still stops it) from
    "claimed" (it won't), and "storing" from "running".
- **Kept small:**
  - Three queries and three mutations, covering what tools need: start runs and follow them.
  - No subscriptions: tools poll `run(id)`. A subscription needs a websocket transport and a
    change feed the executor doesn't have, and polling a snapshot is enough for runs that
    take seconds to minutes.
  - No pagination yet (K60).
- **Library:** `graph-gophers/graphql-go`. It is schema-first, so the SDL is the design I
  defend, and the resolvers are plain struct fields. gqlgen would have added a generator
  and thousands of generated lines.

---

## 4. What I did to break it

### Method

Each experiment followed the same steps:
1. Write the prediction down first.
2. Inject the fault.
3. Compare the record with every ledger and with storage.
4. Log it, including where the prediction was wrong.

From C4 on, `scripts/check-run.py` does step 3. For every step and attempt, it checks:
- the outcome and times match the ledger;
- every ledger execution is an attempt in the record;
- no job ran twice;
- every completed read's object has the ledger's hash.

The checker was itself tested against doctored ledgers, and it flagged all six kinds of
mismatch.

### Experiments

| | Fault | What I saw | What changed because of it |
| --- | --- | --- | --- |
| **C1** | Partition of a bare NATS client: default and 2s pings, 6s and 20s | Prediction wrong twice. Late, not lost, with default pings; 7 of 8 "timed-out" JetStream publishes were stored | The whole of D1: claim, same-id resend, Msg-Id, explicit pings |
| **C2** | Storage stopped mid-read, plate reader restarted before its upload, a late upload put back by hand | Driver retried, then reported `data_lost` after restarting. The read stayed open, and the late object completed it within one recheck | D3. The first version had failed the run, and the late upload then contradicted the record |
| **C3** | `LH/INC/PR_HANG_PCT=30`, `CHAOS_SEED=0` and `17` | Every prediction held. Hung liquid-handler and incubator steps failed with "not repeated"; a hung plate read was repeated and completed | K53: REST hides an earlier attempt's device time. Fixed in GraphQL (§3) |
| **C4** | 26 runs: executor killed mid-step and mid-read; each driver killed mid-job or idle; executor and plate reader partitioned; storage down 15s; `chaos.sh random --seed 7` and `--seed 11`; the same with instrument faults at `CHAOS_SEED` 3, 13 and 29 (chosen from a computed roll table) | **Every record true** (`check-run.py`: 26 ok, 0 problems). A driver killed mid-job: its ledger closes the job `hung` at the restart; the plate reader retried it, the liquid handler failed the step with its reason | K62 logged, not fixed: it is latency, not truth |

**Exact C4 commands:**
- **One fault per run:** `scripts/experiments/c4-fault.sh <a-h>`.
- **Random faults:** `FOR=90 scripts/experiments/c4-random.sh <seed> <out>`.
- **Instrument faults:** set by
  `CHAOS_SEED=13 INC_FAIL_PCT=20 PR_HANG_PCT=30 HANG_DURATION=5s docker compose up -d liquid-handler-1 incubator-1 plate-reader-1`.

### Where my predictions were wrong in C4

C4's wrong predictions were about time, not truth:

- **An 8s executor partition delayed the run by about 30s.**
  - Measured: two events sat `ack_pending` on the consumer for exactly `ackWait`.
  - Why: they had been handed to the executor's pull request just as it was cut off, so
    JetStream only redelivered them after `ackWait`.
  - Meanwhile the record said `running` for steps already finished: behind, not false (K62).
- **A storage outage of any length costs at least about 16s.** SeaweedFS needs that long to
  serve S3 again. The driver's upload succeeded on its next 0.5s retry.
- **The first instrument-fault seed was a vague prediction.** I hadn't computed the roll. The
  roll depends on seed, device, step and attempt, never the run, so one seed gives every run
  the same faults.

### Other checks behind each change

- **A failing test first.** Every behaviour started as one.
- **Mutation checks:** deliberately broken versions of the code, to confirm the tests catch
  them.
- **`go test -race ./...`.** It catches a driver bug the plain suite misses (K44).
- **`acceptance.sh`** after every slice. It passes 7/7 on both workflows with faults off.

---

## 5. What is still not true, or not proven

None of these makes the record say something false. They are liveness, assumptions, and
things not proven.

### Deliberate boundaries

- **Work already started is never stopped.** Not by a cancel, and not by a timer. Only its
  evidence says how it ended.
- **No operator tools** (K6, K39). The consequence:
  - A read whose data was lost, and whose re-reads also failed or lost their data, stays
    `running` for ever.
  - That is true: an earlier upload may still land, and SeaweedFS can't make loss final
    (D3). But a run can then never end.
  - The fix is designed (D4: an `abandonStep` mutation) but not built.
- **A driver that never comes back** leaves its claimed step `running`. It is unknown, so
  the record says so (K6).
- **Times of a crashed attempt** end at the instrument's restart, not when work actually
  stopped. That is the ledger's time, the best evidence there is.

### Assumptions the guarantees rest on

- **One driver process per instrument, and a ledger that survives restarts (K55).**
  "Resending never runs a job twice" depends on both.
- **One executor (K56).** Withdrawing a claim after a cancel assumes one executor answers a
  driver's claims in order. With replicas, a stalled claim could withdraw an attempt that is
  running. The fix would be for drivers to number their claims.
- **The instrument's end-of-job ledger write succeeds (K16).** If it fails, the instrument
  says `completed` now, but a restarted instrument says `hung`. That can't be seen from
  outside the instrument (`instrument.go:241`).
- **A single NATS server, no retention limits on `LAB_EVENTS` (K22), and no rolling
  upgrade.** A v2 driver against a v1 executor is rejected: it fails safe (K14).

### Known gaps

- **`acceptance.sh` check 5** ("no step ran more than once") fails on any run that retried
  (K5). Each attempt is its own ledger execution, by design. The check is meant for the
  happy path, and it passes there.
- **REST `GET /runs/{id}` doesn't read from one snapshot (K59).** For one poll it can show a
  run and its steps from different moments, such as `running` beside steps that have all
  ended. GraphQL reads in a snapshot.
- **`createRun` has no idempotency key (K58).** A tool that retries after a lost response
  creates a second run.
- **A partition of the executor costs up to 30s** (`ackWait`, K62). A retry after a failure
  is immediate, with no backoff (K51).
- **Step and attempt times differ below 1µs (K61):** Postgres stores microseconds; the
  lifecycle keeps the ledger's nanoseconds.

### Not proven

- **C1 can't be fully rerun from the repo.** Its probe was a local-only script (K8).
- **C2 ran before D4.** Today a lost read is also re-read at once. Unit tests cover that, but
  it hasn't been re-run live.
- **Several runs competing for one device.** Chaos ran runs back to back, not concurrently.
  There is no fairness across runs (K29).
- **Scale.** The dispatch loop rescans running runs every 500ms, replay sends the whole
  ledger on every start, and lost reads are rechecked every 10s for ever. All fine at three
  devices, not measured beyond (K20, K28, K35, K40, K50).
- **One test gap (K38).** That replay uses the ledger as it was before listening is shown by
  the code, not by a test.

The full list, with what each would take, is in `docs/forengineer/concerns.md`: 29 open, 9 accepted.

---

## 6. How long I spent

<!-- TODO(user): total time actually spent, roughly. Not derived from commit times. -->

_To be filled in._

---

## What I thought of the exercise

<!-- TODO(user): what was unclear, what was annoying, what you would change. -->

_To be filled in._

---

## AI use

- **Claude Code wrote the code and the docs.** It worked to the conventions in `CLAUDE.md`
  and the `/decide`, `/slice` and `/break` skills in `.claude/skills/`.
- **I made every design decision and reviewed every PR.** Each design fork was a `/decide`,
  with the alternatives argued before I chose, and several were reversed in my review:
  - D3's first version failed a lost read after a grace period;
  - D4's first draft used one retry trigger for all causes, and treated the incubator as
    repeat-safe;
  - for D5's run status, rule 2 was rejected because it hides a failure that happened anyway.
- **Codex** reviewed each PR read-only, once (`scripts/codex-review.sh`). Its findings and how
  each was handled are in the PR descriptions and `docs/forengineer/concerns.md`.
- **The conversations are in `ai/`.**
  <!-- TODO(user): export, review for anything private, and say here if anything was left out. -->
