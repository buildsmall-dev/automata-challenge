# Design decisions

The six design decisions behind the executor and the drivers. Each says what problem it
solves, what we chose, how it works, why not the alternatives, and where it falls short.

This page shows the design **as it is now**. The working log in
[`../forai/decisions.md`](../forai/decisions.md) keeps the history: the decisions as
first recorded, what changed later, and the review notes. The numbers D1–D6 (decisions),
K1–K62 (concerns) and C1–C4 (experiments) are the same in both.

| | Decision | In one line |
| --- | --- | --- |
| [D1](#d1-messaging) | Messaging | Commands are short-lived requests, results go through a durable stream, and Postgres decides. |
| [D2](#d2-a-verified-read-stays-completed) | A verified read stays completed | Once a read's data is verified in storage, the step never goes back. |
| [D3](#d3-lost-data-is-an-open-question) | Lost data is an open question | "Not in storage now" is not "lost", so a lost read stays unresolved. |
| [D4](#d4-retries) | Retries | Retry only when there is a positive reason to think repeating is safe. |
| [D5](#d5-cancel) | Cancel | Stop what hasn't started, keep the evidence of what has, and say why the run stopped. |
| [D6](#d6-graphql) | GraphQL | A second view of the same rows, adding every attempt. |

## Terms used on this page

- **Run, step:** a run is one execution of a workflow; a step is one operation in it, on
  one device.
- **Attempt, job id:** each time a step is sent to a device is an attempt. Its job id is
  `<run>/<step>/<attempt>`, for example `run-1a2b/read_plate/2`. The job id is what the
  instrument records, and what makes a repeated message harmless.
- **Instrument, driver, ledger:** the instrument is the simulated device. The driver is our
  process in front of it. The **ledger** is the instrument's own record of every job: when
  it started, when it ended, and how (`completed`, `failed`, `hung`). It is the best
  evidence of what physically happened.
- **Record:** what the executor stores and serves (REST, GraphQL). The rule for the whole
  project is that **the record must be true**: every claim it makes has evidence in a
  ledger or in storage.
- **Read:** a plate-reader step. It produces data, and it counts as completed only once
  that data is in storage with the sha256 the ledger measured.
- **Claim:** the driver's request for permission to start a job, made just before it
  starts it.
- **Phase:** the executor's internal state of a step, finer than the API status (table in
  D1).

---

## D1. Messaging

**Commands are short-lived requests, results go through a durable stream, and Postgres
decides.**

### The problem

Commands go out to drivers, and results come back, over a network that can lose, delay or
duplicate messages, while any process may restart. Before writing code we measured how NATS
behaves when a client is cut off from the network (experiment C1):

- A request that **timed out could still be delivered**, up to 20s late.
- **7 of 8 JetStream publishes that reported a timeout had in fact been stored.**

So a timeout means "unknown", never "didn't happen". Anything built on "no reply means it
didn't run" would sooner or later run a job twice or record a false outcome.

### How one step travels

```
executor                         driver                      instrument
   |  1. command (request) -------->|                              |
   |<-------- accepted / busy / known                              |
   |                                |                              |
   |<- 2. claim (request) ----------|                              |
   |------------- go / abort ------>|                              |
   |                                |-- 3. Run ------------------->| writes its ledger
   |<= 4. started, finished ========| (JetStream, from the ledger) |
   |                                |-- 5. upload the read's data -> storage
   |<= 6. stored ===================|                              |
   |  7. read the object back, compare its sha256 with the ledger's
```

1. **Command.** Postgres is the queue. Every 500ms the executor looks for steps that are
   ready and sends each one a NATS request on `lab.<device>.cmd.run`, carrying the job id.
   The driver answers:
   - `accepted`: it will try to run the job;
   - `busy`: the instrument is doing something else, so nothing ran;
   - `known`: it already has this job, in flight or in its ledger.

   The executor keeps sending the same command until there is evidence the job started:
   again after 500ms on `busy` or no answer, and after 3s on `accepted` or `known`. A
   repeat always carries the **same job id**, so it can never become a second attempt.
2. **Claim.** Just before starting the job, the driver asks the executor
   (`lab.executor.claim`). The executor records the claim under the step's row lock, the
   same lock a cancel takes, so exactly one of claim and cancel wins. It answers `go` or
   `abort`. If the database fails it doesn't answer, and the driver asks again. A claim is
   only accepted from the device the step was assigned to.
3. **Run.** The driver starts the job on the instrument, which writes it to its ledger
   before anything physical happens.
4. **Events.** The driver publishes `started` and `finished` to the JetStream stream
   `LAB_EVENTS` (subject `lab.<device>.evt.<kind>`), retrying until JetStream confirms.
   Each event carries the ledger's times and outcome. Its message id is `<job id>:<kind>`,
   so JetStream drops a duplicate.
5. **Data.** For a read, the driver uploads the bytes to `runs/<job id>.csv` in storage,
   retrying until storage accepts them. Meanwhile it reports the device busy, because the
   bytes exist only in its memory.
6. **Stored.** The driver then publishes `stored`.
7. **Verify.** The executor doesn't trust `stored`: it reads the object back from the key
   it computes itself and compares its sha256 with the ledger's. Only a match completes
   the step.

The executor reads `LAB_EVENTS` through one durable consumer. For each event it validates
it, applies it to the step under the row lock, and acks it only after the database commit.
A malformed event, or one whose device doesn't match its subject, is rejected for good. A
database error means the event is redelivered. Applying the same event twice changes
nothing.

| Channel | Kind | Sent by | Answered or acked by | If it's lost |
| --- | --- | --- | --- | --- |
| `lab.<device>.cmd.run` | NATS request | executor | driver | sent again with the same job id |
| `lab.executor.claim` | NATS request | driver | executor (queue group) | driver asks again |
| `lab.<device>.evt.*` | JetStream stream | driver | executor, after commit | driver retries until stored; redelivered until acked |
| `runs/<job id>.csv` | S3 object | driver | executor reads it back | driver retries the upload |

### Recovering from crashes

- **The executor restarts.** Its state is in Postgres, and events wait in the stream, so it
  carries on where it stopped.
- **A driver restarts.** It first **replays** its ledger: for each job it reports
  `started` and `finished` again, and a job cut off by the restart is reported `hung` (the
  instrument marks it so). For each completed read it asks storage whether the data is
  there: present → `stored`; absent after a grace period → `data_lost` (D3).
- **A step is claimed, but nothing says it started.** Perhaps the driver died after
  claiming, or the instrument refused the job. The executor keeps sending the same command
  every 3s, and the answer tells it what happened:
  - `known`: the driver has the job, so it ran or is running. Wait for its events.
  - `accepted`: the driver has no record of it, so it never started. The driver claims it
    again, the executor answers `go`, and it runs once.
  - no answer: still unknown, so the step stays `running` and the executor asks again.

### Rules that keep the record true

- **Unknown stays unknown.** A step whose driver can't be reached stays `running`. It is
  never failed on a timer.
- **Late facts always win**, even after a cancel. A late `finished` is still recorded.
- **A read is completed only when its stored data matches the ledger.** Once verified it
  stays completed (D2).
- **A lost read is unresolved, not failed** (D3).
- **A run finishes only when no step is in flight.** Its status comes from D5's rule, and
  its `finished_at` is the last ledger time.

### Step phases

The API has five step statuses. Internally the executor tracks finer phases:

| Phase | API status | Meaning |
| --- | --- | --- |
| `pending` | pending | not sent to a device |
| `dispatched` | pending | command sent, nothing physical yet |
| `retrying` | running | a later attempt sent, not claimed yet; an earlier one ran |
| `claimed` | running | the driver may start it; it can no longer be cancelled |
| `running` | running | the instrument has started it |
| `storing` | running | read completed, its data not yet verified in storage |
| `unresolved` | running | the evidence can't say how it ended; `error` says why |
| `ended` | running | every attempt ended unsuccessfully; retry not decided yet |
| `completed`, `failed`, `cancelled` | the same | final |

### Alternatives we rejected

| Option | Why not |
| --- | --- |
| Request/reply, and the executor polls the driver for outcomes | Results live only in the driver's memory and the ledger while the executor is down. It also assumes we can always ask the instrument, which is true of this simulator but not of most equipment. |
| JetStream for commands too | A durable command queue delivers stale commands: a job could start after a cancel, or after someone moved the plate by hand. It also needs one ack timeout for jobs of 2s to hours. |
| Shared state in NATS KV | A second source of truth beside Postgres, without transactions. |
| Commands that expire, or a cancel broadcast, instead of a claim | Both make the window for a late command smaller, but still race with the cancel. Expiry also depends on synchronised clocks. |

### What it depends on

- The instrument writes its ledger before work starts, and sets the outcome before `Run`
  returns (`instrument.go:166-170, 226-228`). *Inferred from the code.*
- A timeout doesn't mean "not delivered", and a failed JetStream publish may be stored.
  *Measured in C1.*
- One driver process per instrument, and an instrument ledger that survives restarts (K55).
- One NATS server and one executor.

### Limits

- A driver that never comes back leaves its step unknown for ever. There is no operator
  action to declare it lost (K6).
- JetStream drops duplicates for 2 minutes. After that, a repeat relies on applying events
  being idempotent, which they are.
- Replay reports the whole ledger on every start (K35). Fine at this scale.
- If the executor is cut off from NATS, events in flight are redelivered only after the
  30s ack timeout (K62, measured in C4).

**Explainer:** [`../html/d1-messaging-v2.html`](../html/d1-messaging-v2.html) (as first
designed; the event names and data key changed later). **Built in:** PRs #4–#11 and #18.

---

## D2. A verified read stays completed

**Once a read's data is verified in storage, the step never goes back.**

### The problem

A read is completed once a read-back of its object matches the ledger's sha256. What if a
later read-back doesn't match, because the object was changed or corrupted?

### Decision

The step stays `completed`, with its `data_ref`. "Completed, data verified at time T" stays
true for ever. The problem is reported instead: REST's `error` says "stored data no longer
matches what the instrument measured (checked T)", and a newer matching read-back clears
it. GraphQL shows every read-back, with a `matches` flag, so the warning is its own field
there (K1).

Before the first match, a mismatch leaves the step `unresolved`.

### Why not the alternatives

- **Move the step back to unresolved.** It would still be true, but a status would go
  backwards, and the steps after it may already have run on that data.
- **Ignore read-backs after completion.** It hides corruption.

### What it depends on

Evidence is a set of read-backs, so "some read-back matched" doesn't depend on the order in
which they arrive. A test checks every ordering.

**Built in:** the step lifecycle (PR #4).

---

## D3. Lost data is an open question

**"Not in storage now" is not "lost", so a lost read stays unresolved.**

### The problem

When a driver restarts, it no longer has the bytes of a read it hadn't finished uploading.
If storage doesn't have the object either, the driver reports `data_lost`. Should the read
then fail, and its run with it?

### Decision

No. `data_lost` makes the read **unresolved**: the API says `running`, and `error` says the
data was lost but an earlier upload may still appear. The run stays open.

- Every 10s the executor looks in storage for each such read. A matching object completes
  it, exactly as if `stored` had arrived.
- Ending it any other way is a decision, never a timeout. Today that decision is the retry
  policy (D4): the plate reader reads the plate again at once.

### Why

The dead process's upload may still land. Nothing in S3 or SeaweedFS bounds how late that
can be. *Inferred:* no such promise exists in either. If we failed the read and the object
then appeared, the record would contradict storage. That is exactly what happened in an
early version, which failed the read after a grace period (seen in C2).

### Alternatives we rejected

| Option | Why not |
| --- | --- |
| Fail after a grace period | A delayed upload can land after it, and then the record is false. |
| Make storage the referee: write only if absent (`If-None-Match: *`), and write a tombstone before declaring loss | That would make "lost" final. But SeaweedFS 3.62 ignores the header: a second conditional write returned 200 and overwrote. *Measured.* |
| Give each process its own key, so a late upload can't land on the read's key | The data would still be in storage, so "lost" would still be false. |

The driver keeps a grace period (2 × the 5s storage timeout) before reporting `data_lost`.
That only avoids a needless report; correctness doesn't depend on it.

### Limits

If the re-reads also fail, the read stays unresolved and its run `running` until the
object appears. Nothing ends it, because the operator action was not built (K39). This is
the main open limit: the record stays true, but a run can stay `running` for ever.

**Would change if** storage supported conditional writes (newer SeaweedFS, AWS S3). Then a
tombstone would make loss final, and a lost read would be retried like a hang.

**Built in:** driver replay (PR #11). Tested live in C2.

---

## D4. Retries

**Retry only when there is a positive reason to think repeating is safe.**

### The problem

Some steps can't succeed as they stand: the instrument failed, it hung, or a read's data
was lost. Without a policy, a hung or lost step stays `running` for ever, and any failure
fails the run. But a careless retry could dispense liquid twice into the same plate.

### Decision

Retry automatically, at most 3 attempts per step in total. Each cause has its own trigger,
and each trigger needs a positive statement that repeating is safe:

| Cause | Retried when | Who says it's safe | Otherwise |
| --- | --- | --- | --- |
| The instrument failed | the failure is marked retryable | the instrument (`Retryable`) | `failed` |
| The instrument hung | the device is in `REPEAT_SAFE` | our configuration | `failed`: "instrument hung; outcome unknown; not repeated" |
| A read's data was lost | the device is in `REREAD_ON_DATA_LOSS` | our configuration | stays unresolved (D3) |

Details:

- **Failure.** The driver passes on the instrument's `Retryable` flag in `finished`. The
  ledger doesn't store the flag, so after a driver restart, replay reports the failure
  without it, and the step is not retried. The same failure can end differently around a
  restart, but always on the side of not repeating (K43).
- **Hang.** There is no timer. The retry waits for the instrument's own report that it
  gave up (after `HANG_DURATION`), so it is never sent to a frozen instrument. That makes
  a retried hang cost the full `HANG_DURATION` (30s by default).
- **Data lost.** The read is repeated at once, while the lost attempt stays open (D3). The
  first attempt to be verified completes the step.
- **Defaults:** `REPEAT_SAFE=plate-reader`, `REREAD_ON_DATA_LOSS=plate-reader`,
  `MAX_ATTEMPTS=3`. Reading a plate again leaves the plate unchanged. Repeating a hung
  liquid transfer, or a hung incubation, would add an unknown amount to the same plate.
  The incubator still retries a failure the instrument marks retryable.

**A step is decided by all its attempts:**

- `completed` once any attempt is verified complete. The first verified attempt supplies
  `data_ref` and the times.
- `failed` only when every attempt has ended, none completed, and no further attempt will
  be made. `error` ends with the reason the policy gave up.
- otherwise `running`.

Every attempt is kept in the record. REST shows the attempt that decided the step (or the
latest, if none has). GraphQL shows all of them.

### Alternatives we rejected

| Option | Why not |
| --- | --- |
| An operator decides every retry | Never repeats anything by itself, but under any fault, runs never end on their own. |
| Use only the instrument's `Retryable` flag | A hang has no flag, so the hardest case is left undecided, or retried unsafely. |
| One trigger for all causes, and "a step is its latest attempt" (first draft) | Wrong for lost data. The first attempt's upload can still land, so a failed re-read would have marked the step `failed` while it might still complete. |
| Leave a final hang `running` until an operator acts | `running` would claim work is happening after the instrument has stopped. |
| Make the incubator repeat-safe | Triple Assay would finish under hang faults, but by incubating a plate twice for an unknown total time. |

### What it depends on

- The fault roll is `sha256(seed:device:step:attempt) % 100`, with no run id in it. So,
  under one seed, a step's first attempt has the same fate in every run; only a retry
  rolls again. *Measured.* This is why experiments can predict outcomes from a table.
- After the ledger says `hung`, nothing more can be learned about that job
  (`instrument.go:192-199`). *Inferred.*
- `Retryable` exists only in `Run`'s return value (`instrument.go:211`), not in the ledger.
  *Inferred.*
- Which operations are safe to repeat on real equipment. *A guess.* That's why it's a
  setting per device, not code.

### Limits

- If every re-read after a data loss also fails, the step stays `running` (K39).
- A retried step is two jobs in the ledger, so the acceptance script's check 5 ("no step
  ran more than once") fails under faults. That is honest, not a bug (K5).
- Retries go at once, with no backoff (K51). A fault lasting more than a few seconds uses
  up the 3 attempts quickly.
- A re-read is a second measurement, taken later.

**Explainers:** [`../html/d4-hang-retry.html`](../html/d4-hang-retry.html) (first draft),
[`../html/d4-hang-retry-v2.html`](../html/d4-hang-retry-v2.html). **Built in:** PRs
#13–#16. Tested live in C3 and C4.

---

## D5. Cancel

**Stop what hasn't started, keep the evidence of what has, and say why the run stopped.**

### The problem

What does `CancelRun` do to each step, and what is the run's status once the work still in
flight has ended? The hard case: a step claimed by a driver that then died, before the job
started (K54).

### What a cancel does

The whole cancel is one transaction. It locks every step of the run in a fixed order
(by position, so two multi-step transactions can't deadlock), and cancels each:

| The step was… | After the cancel |
| --- | --- |
| not sent | never will be |
| sent, not claimed | `cancelled`. Exactly one of claim and cancel wins (same row lock). |
| claimed or running | not stopped: it may be physically running. Its events are still recorded. |
| failed, and would have been retried | not retried. `failed`, "…; not retried: cancelled" |
| a lost read | not read again. It stays open until its upload appears (D3). |

A run that hasn't started is cancelled at once. A finished run is left as it is.

**Withdrawn claim (K54).** If a driver asks again for a claim it was already granted, after
a cancel, and there's no evidence the job started, the executor withdraws the claim and
answers `abort`. The step can then be `cancelled`. This is safe because a driver asks for
a claim only before it runs a job, and only for a job its ledger doesn't have, so the
request itself proves the job never started. The resend (D1) is what makes the driver ask
again.

### The run's final status

Once nothing is in flight:

- `completed` if every step completed (the cancel came too late to stop any);
- `failed` if a step failed for its own reason;
- `cancelled` otherwise: the cancel is why the rest didn't happen.

The retry policy decides whether a cancel is the reason a step ended. It names the cancel
only if it would otherwise have made another attempt. A failure that was final anyway (not
retryable, not safe to repeat, attempts used up) keeps its own reason, so the run is
`failed`.

### Alternatives we rejected

| Option | Why not |
| --- | --- |
| `cancelled` whenever a cancel was requested and some step didn't complete | Hides a failure that happened on its own. |
| Stop resending a claimed step once it's cancelled | A step whose driver died would stay `running` for ever: the resend is the only way to learn the job never started. |
| A `cancel_requested` column on the run | The steps already record the cancel, so no schema change is needed. |

### What it depends on

- The instrument writes its ledger before anything physical happens, and the driver
  claims only before running a job. *Inferred* from `instrument.go` and `adapter.go`, and
  *measured* live in the cancel PR.
- One driver per instrument (K55) and one executor (K56). With two executors, a stale
  first claim could be mistaken for a re-claim, and withdraw an attempt that is running.

### Limits

Work that has started is not stopped, by design. A cancelled run with a lost read that
never reappears stays `running` (K39).

**Built in:** PR #19.

---

## D6. GraphQL

**A second view of the same rows, adding every attempt.**

### The problem

A second API that kept its own state, or worked the record out differently, could
contradict REST.

### Decision

- **No state of its own.** A step's fields are the columns REST reads, and its attempts
  come from the same row. A run and all its steps are read in one repeatable-read,
  read-only transaction, so they are seen as of one moment. Mutations call the same
  `Executor` methods as REST, then read the record again.
- **What it adds to REST:**
  - the internal `phase` (D1's table) and `cancelRequested`;
  - every attempt, with its own ledger times, outcome, retryable flag and, if unresolved,
    why (K53);
  - for reads, the ledger's digest and every storage read-back, with `matches` and
    `verified` (K1).
- **Operations:** queries `run(id)` (null if unknown), `runs(status)` and `workflows`;
  mutations `createRun`, `startRun` and `cancelRun`. Errors carry `extensions.code`:
  `NOT_FOUND`, `BAD_USER_INPUT`, `WORKFLOWS_INVALID` or `INTERNAL`. The detail of an
  internal error goes only to the log, as in REST.
- **Library:** `graph-gophers/graphql-go`, schema first. The schema in `graphql.go` is the
  design, and resolvers are plain struct fields.
- **Following a run:** clients poll `run(id)`.

### Alternatives we rejected

| Option | Why not |
| --- | --- |
| gqlgen | Type-safe, but adds a code generator and thousands of generated lines. |
| Subscriptions | Need a websocket transport and a change feed the executor doesn't have. Polling is enough for runs that take seconds to minutes. |
| Attempts in their own table | The step's row already holds them, and a copy could drift. |

### What it depends on

- A step's columns and its attempts are written in one `UPDATE` (`store.go`). *Inferred.*
- Each attempt's times are the ledger's, carried by the events. *Measured:* three hung
  plate-reader attempts matched the three ledger executions, while REST showed only the
  last.

### Limits

- `runs` has no pagination (K60).
- REST's `GetRun` is not read in one snapshot, so for one poll it can show a run `running`
  beside steps that have all ended (K59). GraphQL doesn't have this problem.

**Built in:** PR #20.
