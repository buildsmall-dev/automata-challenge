# NOTES

## What this is

The executor runs lab workflows on three simulated instruments: a liquid handler, an
incubator and a plate reader. A run is a small graph of steps, such as "fill the sample
plate", "incubate it", then "read it". Each step runs on one instrument, through a driver
process that sits in front of it.

The hard part is not running the steps. It is keeping the **run record** true while things
go wrong. Messages between the executor and the drivers can be lost, delayed or delivered
twice, and any process can crash at any moment. The record is what a scientist relies on, so
it must never say something happened unless there is evidence that it did:

- **How a step ran, and when:** the instrument's own ledger of every job it ran.
- **A plate read's data:** an object in storage whose sha256 matches the one the ledger
  measured.
- **No evidence, no claim:** while the outcome can still be learned, the record says
  `running`. Once it can't, it says `failed` and gives the reason. It never says `completed`
  without evidence.

### How to read this note

Each section stands on its own. Claims carry one of three labels where it matters:

- **measured:** I ran it and saw it.
- **inferred:** I read it in the code (ours, or the fixed `instrument.go`).
- **assumed:** the design depends on it, but nothing here checks it.

More detail is in:

| | |
| --- | --- |
| [`docs/html/system-overview.html`](docs/html/system-overview.html) | Interactive walkthrough: the topology, one run step by step, and six failure scenarios |
| [`docs/forengineer/decisions.md`](docs/forengineer/decisions.md) | Every design decision (D1–D6) with its alternatives |
| [`docs/forengineer/chaos-log.md`](docs/forengineer/chaos-log.md) | Every experiment (C1–C4): prediction, result, lesson |
| [`docs/forengineer/concerns.md`](docs/forengineer/concerns.md) | Everything noticed along the way (K1–K62), by status |
| [`docs/forai/`](docs/forai/) | The same three records as written during the work, with their history |
| `scripts/check-run.py` | Compares a run's record with all three ledgers and with storage |

---

## 1. Topology

### The main idea

One rule shaped the design: **durable facts, short-lived intentions, and Postgres decides.**

- **Intentions** ("run this job") travel as plain NATS requests. They aren't stored anywhere
  on the way. If one is lost it is simply sent again. A command must never sit in a queue
  and run later, after a cancel, or after someone has moved the plate.
- **Facts** ("this job started", "it finished at 12:00:10") travel through a JetStream
  stream. They are kept until the executor has saved them, so they survive the executor
  being down.
- **Postgres decides.** The executor applies every fact and every decision in one database
  transaction. Postgres is the only place that says what should happen next.

### The pieces

```
     Postgres  ◄──── one transaction per fact ────  Executor  ── reads data back ──►  S3 storage
  runs, steps,                                     API, dispatch loop,                 runs/<job id>.csv
  every attempt                                    claims, events                            ▲
                                                    │   ▲        ▲                           │
                                      (1) command   │   │ (2)    │ (3) events                │
                                                    ▼   │ claim  │                           │
                                            Core NATS            JetStream LAB_EVENTS        │
                                       (request/reply, not      (stream, on disk)            │
                                             stored)                     ▲                   │
                                                    │   ▲                │                   │ (4) upload
                                                    ▼   │                │                   │
                                     Driver (one per instrument) ────────┴───────────────────┘
                                         │ in the same process
                                     Instrument ──► its ledger (a file on disk; survives restarts)
```

| What | Where it lives | Survives a restart? |
| --- | --- | --- |
| What should run, every attempt, every fact received | Postgres (`steps.lifecycle`) | Yes. The source of truth for intent |
| What physically happened, and when | Each instrument's ledger, on disk | Yes. The source of truth for physical facts |
| Facts on their way to the executor | JetStream stream `LAB_EVENTS` | Yes |
| Plate data | S3, at `runs/<job id>.csv` | Yes |
| A read's bytes, before they are uploaded | Driver memory | **No.** The one place data can be lost (see §2.7) |
| Commands and claims | Core NATS, in flight only | **No.** They are sent again |

There is no queue in NATS. The queue is the step table in Postgres.

### What goes over the wire

| | Subject | Kind | From → to | Reply or acknowledgement |
| --- | --- | --- | --- | --- |
| 1 | `lab.<device>.cmd.run` | NATS request | executor → driver | `accepted`, `busy` or `known` (the driver already has this job). No reply means *unknown* |
| 2 | `lab.executor.claim` | NATS request, queue group `executor` | driver → executor | `go` or `abort`. No reply if the database fails |
| 3 | `lab.<device>.evt.<kind>`: `started`, `finished`, `stored`, `data_lost` | JetStream stream `LAB_EVENTS` | driver → executor | The driver retries until JetStream confirms. The executor acks only after its database commit |
| 4 | `runs/<run>/<step>/<attempt>.csv` | S3 PUT | driver → storage | The executor reads the object back and hashes it |

Every job has an id `<run>/<step>/<attempt>`, for example `run-3c9e/read_plate/1`. It stays
the same however many times a message about it is sent, and it is the unit of idempotency
throughout: the driver answers `known` to a repeated command, JetStream drops a repeated
event by its message id (`<job id>:<kind>`), and the storage key comes from it.

### A normal run, step by step

Take `read_plate`, the last step of PCR Amplification, on `plate-reader-1`:

1. **Dispatch.** The dispatch loop runs every 500ms, and at once when a new fact arrives. It
   asks Postgres which steps are ready. `combine` has completed, so `read_plate` is ready.
   The executor records attempt 1 in Postgres *first*, then sends the command.
2. **Command.** The driver answers `accepted`. The executor keeps sending the same command
   until there is evidence the job started: after 500ms on `busy` or no answer, and every 3s
   on `accepted` or `known`.
3. **Claim.** Just before touching the instrument, the driver asks the executor for
   permission. The executor records the claim under the step's row lock and answers `go`.
4. **Run.** The instrument writes the job to its ledger before doing anything physical. The
   driver reads the ledger every 50ms and publishes `started`, with the ledger's time.
5. **Finish.** The ledger says `completed`, with the sha256 of the data. The driver publishes
   `finished`, with the outcome, both times and the hash.
6. **Upload.** The driver uploads the bytes to `runs/run-3c9e/read_plate/1.csv`, retrying
   until storage accepts them. Then it publishes `stored`.
7. **Verify.** The executor doesn't trust `stored`. It reads the object from the key it
   computes itself and hashes it. The hash matches the ledger's, so the step is `completed`
   and gets its `data_ref`.
8. **Finish the run.** No step is in flight and every step completed, so the run is
   `completed`. Its `finished_at` is the last ledger time, not the moment the executor
   noticed.

What the API says along the way, and what backs it:

| After step | `status` | `started_at` / `finished_at` | `data_ref` | Backed by |
| --- | --- | --- | --- | --- |
| 1–2 | `pending` | — | — | our intent only |
| 3 | `running` | — | — | the claim: permission, not evidence |
| 4 | `running` | from the ledger / — | — | the ledger: it started |
| 5–6 | `running` | from the ledger | — | the ledger says completed, but the data isn't verified |
| 7 | `completed` | from the ledger | `s3://lab-data/runs/…/1.csv` | the ledger, plus an object with the same hash |

The executor tracks finer phases than the API shows. `dispatched`, `claimed`, `storing`,
`unresolved` and others are visible in GraphQL (§3).

Every event is applied in one transaction under the step's row lock, then acked. An event
that can't be applied is handled by kind:

- **Malformed, or from a device that doesn't match its subject:** rejected for good.
- **A database error:** redelivered after 1s.
- **A `stored` object that can't be read yet:** redelivered after 2s.
- **For a job no step has:** acked and logged.

### When something goes wrong

One recovery mechanism appears in several rows. When a driver restarts, it first
**replays** its ledger: it reports how every job ended, again (repeats are harmless), and
checks storage for the data of each completed read. Each row is explained in §2, and most
were tested in §4.

| What fails | What happens | What the record says meanwhile | Evidence |
| --- | --- | --- | --- |
| A command is lost, or gets no reply | Sent again with the same job id | Unchanged | measured (C4e) |
| The driver dies after its claim, before starting | Resent every 3s. The restarted driver has no record of the job, so it accepts it, claims it again, gets `go`, and runs it once | `running`, no start time | measured (resend PR #18) |
| The driver dies mid-job | On restart the instrument marks the job `hung`, ending at the restart. Replay reports it. The plate reader repeats it; the liquid handler doesn't | `running`, then the retry's outcome, or `failed` with the reason | measured (C4c, C4d) |
| The instrument hangs | After 30s the ledger says `hung`. Then as above | `running`, then as above | measured (C3) |
| The driver dies before uploading a read | Replay finds no object, and reports `data_lost`. The read stays open, is read again, and storage is checked every 10s | `running`, with the reason in `error` | measured (C2), re-read by unit tests |
| The executor is down or cut off | Events wait in JetStream. A driver asking for a claim gets no answer, so nothing starts. On return it resumes from Postgres | API unavailable, then correct but late | measured (C4a, C4b, C4f) |
| Storage is down | The driver retries the upload, and keeps the plate reader busy | `running` until the data is verified | measured (C4h) |

---

## 2. Why this topology

### 2.1 I measured first

Before writing any code, I cut a bare NATS client off the network, with a peer that stayed
connected. Every second it sent a request (2s timeout) and a JetStream publish (5s timeout).
Runs: 6s and 20s cuts, with default pings and with 2s pings. **My prediction was wrong
twice** (measured, C1):

- **Requests that timed out were still delivered,** up to 20s late. With default pings the
  TCP connection survives the cut, so nothing was lost, but everything was late, including
  requests the sender had already given up on.
- **7 of 8 JetStream publishes that reported a timeout had in fact been stored.**

So **a timeout means "unknown", never "it didn't happen".** Anything that treats a timeout as
"not delivered", such as "no reply, so start a new attempt", would sooner or later run a job
twice or record a false outcome. The decisions below are what follows from that.

### 2.2 Commands as requests, facts as a stream

**Problem.** How should commands reach the drivers, and outcomes come back, so that the
record stays true through lost, repeated and late messages?

**Choice.** Commands are core NATS requests. Facts go through JetStream. Postgres decides.

| Option | How it works | Why not |
| --- | --- | --- |
| **Chosen: requests for commands, a stream for facts** | as above | |
| Requests only; the executor polls the drivers for outcomes | simplest | While the executor is down, facts live only in driver memory and the ledger. It also assumes the instrument can always be asked, which is true of this simulator but not of most equipment |
| JetStream both ways | commands in a durable queue | A stored command can run late: a dispense after a cancel, or after the plate was moved by hand. One ack timeout would have to fit jobs of 2 seconds and of hours |
| Shared state in NATS KV | drivers and executor read and write it | A second source of truth beside Postgres, without transactions |

**Trade-offs.**

- An extra round trip per job, for the claim (§2.3).
- JetStream delivers **at least once, not exactly once.** A repeat within 2 minutes is
  dropped by its message id. After that, applying the same fact twice changes nothing,
  because each fact is applied idempotently.
- When the executor is cut off, events in flight are redelivered only after the 30s ack
  timeout. An 8s cut delayed a run by 30s (measured, C4f). The record was behind, never
  false.

### 2.3 Claim before run

**Problem.** A command can arrive late, and a cancel can arrive at any moment. How do we
make sure a job never starts after it was cancelled?

**Choice.** Before calling the instrument, the driver asks the executor for permission (a
*claim*). The executor records it under the step's row lock, the same lock a cancel takes.
Exactly one of them gets the lock first:

- **The cancel first.** The claim is answered `abort`, and the job never runs.
- **The claim first.** The job may run. The cancel no longer stops it, because the driver
  may already be starting it. Its outcome is recorded when the events arrive.

If the executor doesn't answer (it is down, or the database failed), the driver doesn't
start. It asks again.

*Example.* A run is cancelled while `fill_buffer_plate` waits for the busy liquid handler.
The liquid handler frees up a moment before the cancel, and the driver accepts the command.
Its claim arrives just after the cancel commits, so it is answered `abort`. The ledger never
shows the job.

**Why not the alternatives.** Commands that expire, or a broadcast "cancel" message, only
make the window for a late command smaller. Both still race with the cancel, and expiry also
depends on synchronised clocks. The lock closes the window.

**Trade-off.** It rests on one executor answering claims (assumed; see §5).

### 2.4 A repeated message never becomes a new attempt

**Problem.** A step is claimed, but no `started` arrives. Did the driver die before starting,
did the instrument refuse the job, or is the event just late? Guessing wrong either loses a
job or runs it twice.

**Choice.** Keep sending the **same command, with the same job id**, every 3s. The driver's
answer is the evidence:

- **`known`:** the driver has the job, in flight or in its ledger. It ran, or is running.
  Wait for its events.
- **`accepted`:** the driver has no record of it. The instrument writes its ledger before
  doing anything physical (inferred, `instrument.go`), so the job never started. The driver
  claims it again, the executor answers `go`, and it runs once.
- **No answer:** still unknown. The step stays `running` and the command is sent again.

*Example* (measured, PR #18): the plate-reader driver was killed after its claim was
granted, before it started the job. The step said `running` with no start time. After the
restart the resend was accepted, and the ledger shows the job ran exactly once.

A new attempt, with a new job id, is only ever started by the retry policy (§2.6), never by
a timeout. I first designed a separate "what do you know about this job?" query. The command
already asks exactly that, so the query would have added a subject and driver code to learn
nothing more.

**Trade-off.** If a driver never comes back, its step stays `running` for ever: we can't
know whether it ran. An operator action to declare it lost was not built (§5).

### 2.5 A read is complete only when its data is verified

**Problem.** The instrument hands a read's bytes to the driver only in memory. The ledger
saying `completed` doesn't mean the data is safe.

**Choice.** A read is `completed` only when the executor has read the object back from the
key it computes itself, and its sha256 equals the ledger's. Until then the step is `running`.
The driver keeps the plate reader busy until the upload succeeds, so its bytes aren't
dropped.

**Once verified, it stays verified.** If a later read-back doesn't match (say the object was
corrupted), the step stays `completed` and the mismatch is reported in `error`. Moving the
status backwards would be true, but steps after it may already have used that data.

### 2.6 Retry only when repeating is known to be safe

**Problem.** Some steps can't succeed as they stand: the instrument failed, hung, or a read's
data was lost. Without retries, every fault fails the run. A careless retry, though, could
dispense liquid twice into the same plate.

**Choice.** A new attempt needs a positive reason to think repeating is safe, from the right
source for its cause:

| Cause | Who says repeating is safe | Default |
| --- | --- | --- |
| The instrument failed | The instrument's `retryable` flag | Liquid handler: no. Incubator and plate reader: yes |
| The instrument hung | The setting `REPEAT_SAFE` | Plate reader only |
| A read's data was lost | The setting `REREAD_ON_DATA_LOSS` | Plate reader only |

- **At most 3 attempts** per step, in total.
- **No timers.** A hang is retried only after the instrument itself reports `hung`, so a
  retry is never sent to an instrument that is still frozen.
- **A step is decided by all its attempts.** It is `completed` once any attempt is verified,
  and `failed` only when every attempt has ended and no more will be made.

*Example: a hang* (measured, C3). With hang faults at 30% and seed 17, `plate_read` hung on
its first attempt. Thirty seconds later the ledger said `hung`. Attempt 2 was sent 31ms after
that, completed, and decided the step. The run took 36s instead of about 6s. With seed 0,
`fill_sample_plate` hung on the liquid handler instead. It wasn't repeated, and the step
failed with "instrument hung; outcome unknown; not repeated: liquid-handler-1 is not safe to
repeat". The other six steps were never sent. That is the honest answer: a hung dispense
may have moved some, all or none of the liquid.

**Why not the alternatives.**

- *An operator decides every retry:* never repeats anything by itself, but under any fault
  setting, runs never end on their own.
- *Use only the instrument's flag:* a hang has no flag, so the hardest case is left
  undecided.
- *Treat the incubator as safe to repeat:* runs would finish under hang faults, but by
  incubating a plate twice for an unknown total time. I rejected this in review.

**Trade-offs.**

- Which operations are safe to repeat on real equipment is a **guess**. That's why it is a
  setting per device, not code.
- A retried step is two jobs in the ledger, so the acceptance script's check 5 ("no step ran
  more than once") fails on any run that retried. The script is meant for the happy path.
- Retries go at once, with no backoff.

### 2.7 Lost data stays an open question

**Problem.** If the driver crashes after a read but before the upload succeeds, its bytes
are gone. The restarted driver finds no object in storage. Should the read fail?

**Choice.** No. "Not in storage now" is not the same as "lost". The driver reports
`data_lost` after an upload grace of 10s, and the read becomes *unresolved*: the API says
`running`, and `error` says the data was lost but an earlier upload may still appear. Then:

- On the plate reader, a re-read starts at once, alongside the open attempt.
- Every 10s the executor looks in storage for the original object.
- Whichever attempt is verified first completes the step.

*Example* (measured, C2). I stopped storage during a read, restarted the plate reader's
driver once the read had completed, then started storage again. The driver reported the
data lost, and the run stayed `running`. I then stood in for a late upload by putting the
same bytes at the read's key by hand. Within one 10s check the read and the run were
`completed`. My first version had failed the read after a grace period. The late upload
then contradicted the record, which is why this decision exists. C2 ran before re-reads
existed: the re-read path is covered by unit tests, not re-run live.

**Why not make the loss final?** Storage could arbitrate with a "create only if absent"
write (`If-None-Match: *`) and a tombstone. But SeaweedFS 3.62 ignores that header: a second
conditional write returned 200 and overwrote the object (measured). Nothing bounds how late
a dead process's upload can land (inferred: neither S3 nor SeaweedFS promises it).

**Trade-off.** If every re-read also fails and the original never appears, the run stays
`running` for ever. It is true, but stuck (§5).

### 2.8 Cancel stops what hasn't started, and keeps what has

**Problem.** What should `CancelRun` do to each step, and what is the run's status once the
work still in flight ends?

**Choice.** One transaction locks every step of the run, in a fixed order so that two such
transactions can't deadlock, then:

| The step was… | After the cancel |
| --- | --- |
| completed | stays completed |
| claimed or running | not stopped (it may be moving liquid); its outcome is still recorded |
| sent, not claimed | cancelled; a later claim gets `abort` |
| pending | cancelled; never sent |
| failed, and would have been retried | not retried: "not retried: cancelled" |

*Example.* About 3s into PCR Amplification, `incubate_samples` and `fill_reagent_plate` are
running, and `fill_buffer_plate` is waiting for the liquid handler. After the cancel, the
two running steps finish and are recorded as completed. `fill_buffer_plate` and the three
later steps are cancelled, and the run ends `cancelled`.

**The run's final status,** once nothing is in flight:

- `completed` if every step completed: the cancel came too late to stop any.
- `failed` if a step failed for its own reason. In the example, a failure of
  `fill_reagent_plate` (liquid-handler failures aren't retryable) would make it `failed`.
- `cancelled` otherwise: the cancel is why the rest didn't happen.

**One hard case: a claim granted to a driver that then died before starting.** The resend in
§2.4 would run the job once the driver is back, even after a cancel. To prevent that, a
second claim for the same job, after a cancel, with no evidence it started, is withdrawn and
answered `abort`. A driver asks for a claim only before running a job, and only for one its
ledger doesn't have, so the second request itself proves the job never started. Measured
live: the step ended `cancelled`, and the ledger had nothing for that job.

**Why not the alternatives.**

- *Call the run `cancelled` whenever a cancel was requested:* that hides a failure that
  happened anyway. I rejected it in review.
- *Stop resending a claimed step once cancelled:* a step whose driver died would then stay
  `running` for ever, because the resend is the only way to learn it never started.

**Trade-off.** Work that has started is never stopped, by design. The withdrawn claim
assumes a single executor (§5).

---

## 3. GraphQL schema

`POST /graphql` on port 5002. The REST API is unchanged. The full schema, with field
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
`INTERNAL`. As in REST, internal detail goes only to the log.

**Why this shape.**

- **It can't contradict REST.** It keeps no state of its own. A step's fields are the same
  columns REST reads, and `attempts` comes from the same row. A run and its steps are read in
  one repeatable-read snapshot. Mutations call the same `Executor` methods.
- **It shows what REST's fixed shape can't:**
  - **Every attempt, with its own ledger times.** After a retry, REST shows only the attempt
    that decided the step. In C3 a hung first read held the plate reader for 30s, and REST
    didn't show it. Measured: three hung attempts in GraphQL matched three ledger
    executions, while REST's `/timeline` showed only the last.
  - **The data evidence:** the ledger's hash and every storage read-back, with `matches`.
    A warning on a completed read gets its own field, instead of sharing REST's `error`.
  - **`phase`:** tells "sent, not yet claimed" (a cancel still stops it) from "claimed" (it
    won't), and "storing" from "running".
- **Kept small.** Three queries and three mutations: enough to start runs and follow them.
  No subscriptions, so clients poll `run(id)`. A subscription would need a websocket
  transport and a change feed the executor doesn't have, and polling is enough for runs
  that take seconds to minutes. No pagination yet.
- **Library: `graph-gophers/graphql-go`.** It is schema-first, so the schema is the design,
  and the resolvers are plain struct fields. gqlgen would have added a code generator and
  thousands of generated lines.

---

## 4. What I did to break it

### Method

Every experiment followed the same four steps:

1. **Predict**, in writing, before running.
2. **Inject the fault** at a chosen moment.
3. **Compare** the record with every ledger and with storage.
4. **Log it**, including where the prediction was wrong.

From C4 on, `scripts/check-run.py` does step 3. For each run it checks that every outcome
and time matches a ledger entry, every ledger execution is an attempt in the record, no job
ran twice, and every completed read's object has the ledger's hash. I tested the checker
against six kinds of doctored ledger, and it caught all six.

A tip that made predictions exact: the instrument picks faults from
`sha256(seed:device:step:attempt)`, with no run id in it. So under one seed every run meets
the same faults, and only a retry rolls again (measured). Computing the roll before running
tells you which steps will fail or hang.

### Experiments

| | What I broke | What I saw | What changed |
| --- | --- | --- | --- |
| **C1** | A bare NATS client cut off the network for 6s and 20s, with default and 2s pings | A timeout doesn't mean "not delivered". 7 of 8 timed-out JetStream publishes were stored | The whole messaging design (§2.1–2.4) |
| **C2** | Storage stopped mid-read; the plate reader restarted before its upload; a late upload put back by hand | The read stayed open, then completed within one 10s check of the late upload | Lost data stays open (§2.7). The first version had failed the run, and the late upload then contradicted it |
| **C3** | Hangs at 30% on every device, seeds 0 and 17 | Every prediction held. Hung liquid-handler and incubator steps failed "not repeated"; a hung plate read was repeated and completed | GraphQL shows every attempt (§3) |
| **C4** | 26 runs: executor killed and cut off; each driver killed mid-job or while idle; plate reader cut off; storage down 15s; random chaos with seeds 7 and 11; instrument faults at seeds 3, 13 and 29 | **Every record true:** `check-run.py` reported 26 ok, 0 problems | Nothing needed fixing. One latency issue logged (below) |

To repeat C4:

```bash
scripts/experiments/c4-fault.sh <a-h>                       # one fault per run
FOR=90 scripts/experiments/c4-random.sh <seed> <out>        # chaos.sh random, runs back to back
CHAOS_SEED=13 INC_FAIL_PCT=20 PR_HANG_PCT=30 HANG_DURATION=5s \
  docker compose up -d liquid-handler-1 incubator-1 plate-reader-1   # instrument faults
```

C1 can't be fully rerun from the repository: its probe was a local script I didn't commit.

### Where I was wrong

In C4, every wrong prediction was about **time, not truth**:

- **An 8s executor cut delayed the run by 30s, not 8s.** While I watched the consumer, two
  events stayed "delivered, not acked" for exactly the 30s ack timeout. JetStream had handed
  them to the executor's pending pull request just as it was cut off, and redelivered them
  only after the timeout. Meanwhile the record said `running` for steps that had already
  ended: behind, not false. A shorter ack timeout would fix it. I logged it rather than
  change it, since it's latency, not truth.
- **A storage outage of any length cost at least 16s.** That's how long SeaweedFS takes to
  serve S3 again after a restart. The driver's upload succeeded on its next retry.
- **My first instrument-fault prediction was vague,** because I hadn't computed the roll. Seed
  3 made every run fail the same way. I then picked seed 13 (to show a retry succeeding) and
  seed 29 (to show a final failure) from a roll table. Both behaved as predicted.

Two other surprises:

- **A driver killed mid-job looks exactly like a hang.** The instrument marks the cut-off job
  `hung` when it restarts, and replay reports it. Its end time is the restart, not when the
  work stopped. That is the ledger's time, the best evidence there is.
- **`report` is also a plate-reader step,** so it stores data too. I missed that in C3's
  prediction. The code handled it.

### Checks behind every change

- Each code change was a small PR into `dev`, built from a check written first, and reviewed
  once by Codex (read-only).
- Mutation checks: I deliberately broke the code to confirm the tests catch it. Surviving
  mutants led to tighter tests.
- `go test -race ./...` on every change. In one mutation check it caught a driver race that
  the plain test suite misses.
- `acceptance.sh` after every change. It passes 7/7 on both workflows with faults off.

---

## 5. What is still not true, or not proven

None of these makes the record say something false. They are about runs that never end,
assumptions, and things I haven't proven. The full list, with what each would take, is in
[`docs/forengineer/concerns.md`](docs/forengineer/concerns.md).

### Not guaranteed

- **A run can stay `running` for ever.** Two cases: a driver that never comes back, and a
  lost read whose re-reads also fail while the original never appears. In both the record
  is true (the outcome really is unknown), but nothing ends the run. I drew this boundary on
  purpose. The fix is an operator action ("declare lost" or "abandon this step") that marks
  the step `failed` with the reason. It was designed but not built.
- **Work that has started is never stopped,** by a cancel or a timer. Only its evidence says
  how it ended. This is deliberate.
- **Delivery is not exactly once.** Messages can arrive twice or late. Stable job ids and
  idempotent apply make that harmless; no part of the design depends on exactly-once
  delivery.
- **The record can lag.** By up to 30s after an executor network cut, and by at least 16s
  after a storage outage (measured).

### Assumptions the guarantees rest on

- **One driver per instrument, and a ledger that survives restarts.** "A resend never runs a
  job twice" depends on both. A wiped ledger, or a second driver for the same instrument,
  would break it. Not reachable in this stack (assumed).
- **One executor.** The withdrawn claim (§2.8) assumes one executor answers a driver's claims
  in order. With two, a stalled first claim could be mistaken for a second one, and withdraw
  an attempt that is running. Drivers numbering their claims would fix it.
- **The instrument's end-of-job ledger write succeeds.** If it fails, the instrument reports
  `completed` and we record it, but after a restart its ledger says `hung`
  (`instrument.go:241`, inferred). Nothing outside the instrument can see that.
- **One NATS server**, and no retention limit on the event stream. A newer driver against an
  older executor is rejected: safe, but no rolling upgrades.

### Known gaps

- **REST `GET /runs/{id}` doesn't read one snapshot.** For one poll, at the moment a run
  finishes, it can show the run `running` beside steps that have all ended. GraphQL reads one
  snapshot.
- **`createRun` isn't idempotent.** A client that retries after a lost response creates a
  second run.
- **Acceptance check 5 fails on any run that retried,** because each attempt is its own
  ledger execution (§2.6).
- **Retries have no backoff,** so a fault lasting more than a few seconds uses up all 3
  attempts.
- **A step's times and its attempts' times differ below a microsecond:** Postgres keeps
  microseconds, the ledger nanoseconds.

### Not proven

- **C1 can't be fully rerun** from the repository (its probe wasn't committed).
- **The re-read after lost data** is covered by unit tests, but wasn't re-run live after C2.
- **Several runs competing for one device.** Chaos ran runs back to back, not at the same
  time. There is no fairness between runs sharing a device.
- **Scale.** The dispatch loop rescans running runs every 500ms, replay resends a driver's
  whole ledger on every start, and lost reads are checked every 10s for ever. Fine with three
  devices; not measured beyond.
- **One property of replay is shown only by the code.** Replay uses the ledger as it was
  before the driver started listening, so a new read is never reported lost. No test would
  catch a change to that.

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

- **Claude Code wrote the code and the docs,** following `CLAUDE.md` and the `/decide`,
  `/slice` and `/break` skills in `.claude/skills/`.
- **I made every design decision and reviewed every PR.** Each design fork was argued with
  its alternatives before I chose. Several first versions were reversed in my review:
  - lost data first failed after a grace period (§2.7);
  - retries first used one trigger for every cause, and treated the incubator as safe to
    repeat (§2.6);
  - a cancelled run was first going to be `cancelled` even when a step had failed on its own
    (§2.8).
- **Codex reviewed each code PR once, read-only** (`scripts/codex-review.sh`). One of its
  findings changed the cancel design: the first version let a cancel relabel a failure that
  was final anyway as "not retried: cancelled". Its findings and how each was handled are in
  the PR descriptions and `docs/forengineer/concerns.md`.
- **The conversations are in `ai/`.**
  <!-- TODO(user): export, review for anything private, and say here if anything was left out. -->
