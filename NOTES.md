# NOTES

The executor runs lab workflows (small graphs of steps) on three simulated instruments: a
liquid handler, an incubator and a plate reader. Each instrument has a driver in front of it.
Messages between the executor and the drivers can be lost, delayed or repeated, and any
process can crash. The goal is that **the run record stays true** through all of that:

- **How a step ran, and when,** comes from the instrument's ledger, its own on-disk record of
  every job.
- **A plate read is complete** only when its data is in storage with the sha256 the ledger
  measured.
- **Without evidence, no claim.** The record says `running` while the outcome can still be
  learned, and `failed` with the reason once it can't. It never says `completed` without
  evidence.

## In two minutes

- **One rule:** durable facts, short-lived intentions, Postgres decides. Commands are plain
  NATS requests that are simply sent again if lost. Facts about what happened go through a
  JetStream stream and wait there until the executor has saved them. Every decision is one
  Postgres transaction.
- **Claim before run.** A driver asks permission just before starting a job, under the same
  database lock a cancel takes, so a job never starts after its cancel.
- **One job id per attempt,** `<run>/<step>/<attempt>`, reused on every resend. A lost reply
  or a crash leads to the *same* job being asked about again, never to a second attempt.
- **Retries need a positive reason** to think repeating is safe: the instrument says the
  failure is retryable, or the device is configured as safe to repeat (only the plate reader,
  by default).
- **Tested by breaking it:** four experiments (crashes, partitions, storage outages, random
  faults), predicted first, then checked against every ledger and storage. In the final pass
  all 26 records were true.
- **Not guaranteed:** a run can stay `running` for ever when the outcome is truly unknown,
  because no operator action to end it was built. The design also assumes one executor and
  one driver per instrument.

Labels used below: **measured** (I ran it), **inferred** (from reading the code),
**assumed** (the design depends on it; nothing here checks it). The full records are in
[`docs/forengineer/`](docs/forengineer/) (decisions, experiments, concerns), and an
interactive walkthrough is in [`docs/html/system-overview.html`](docs/html/system-overview.html).

---

## 1. Topology

```
 Postgres ◄── one transaction per fact ── Executor ── reads data back, hashes it ──► S3 storage
 (runs, steps,                           (API, dispatch loop,                         ▲
  every attempt)                          claims, events)                             │
                                           │  ▲        ▲                              │
                              (1) command  │  │ (2)    │ (3) events                   │ (4) upload
                                           ▼  │ claim  │                              │
                                     Core NATS        JetStream LAB_EVENTS            │
                                           │  ▲        ▲                              │
                                           ▼  │        │                              │
                                      Driver, one per instrument ─────────────────────┘
                                           │ same process
                                      Instrument ──► ledger (file on disk)
```

### What goes over the wire

| | Subject or key | Transport | From → to | Reply or ack |
| --- | --- | --- | --- | --- |
| 1 | `lab.<device>.cmd.run` `{job_id, run_id, step, attempt}` | core NATS request | executor → driver | `accepted`, `busy`, or `known` (already has the job). No reply = unknown |
| 2 | `lab.executor.claim` `{job_id, device}` | core NATS request, queue group `executor` | driver → executor | `go` or `abort`. No reply if the database fails; the driver asks again |
| 3 | `lab.<device>.evt.{started,finished,stored,data_lost}` | JetStream stream `LAB_EVENTS` (file storage), durable pull consumer `executor`, ack wait 30s | driver → executor | The driver retries until JetStream confirms. The executor acks only after its database commit |
| 4 | `runs/<run>/<step>/<attempt>.csv` | S3 PUT, then `stored` | driver → storage | The executor reads it back from the key it computes itself |

- **Duplicates.** Each event's message id is `<job id>:<kind>`, so JetStream drops a repeat
  within its 2-minute window. After that, applying a fact twice changes nothing.
- **Events that can't be applied.** A malformed event, or one from a device that doesn't
  match its subject, is rejected for good. One for a job no step has is acked and logged. A
  database error means redelivery after 1s, and an unreadable `stored` object after 2s.
- **Connection health.** Both sides set NATS pings to 5s, with 2 outstanding.

### Where state lives

| State | Where | Survives a restart |
| --- | --- | --- |
| Intent: runs, steps, every attempt and fact. Also the queue (there is none in NATS) | Postgres, `steps.lifecycle` (JSON) | yes |
| What physically happened | each instrument's ledger, written before work starts | yes |
| Facts in transit | JetStream `LAB_EVENTS` | yes |
| Plate data | S3 | yes |
| A read's bytes before upload | driver memory | **no**: the one place data can be lost |

### A normal step

`read_plate` on `plate-reader-1`, with what the REST API says at each point:

1. **Dispatch.** Every 500ms, or at once when a fact arrives, the executor asks Postgres
   which steps are ready. It records attempt 1 (`run-3c9e/read_plate/1`), then sends the
   command. *API: `pending`.*
2. **Resend until started.** The command is sent again after 500ms on `busy` or no reply, and
   every 3s on `accepted` or `known`. It is always the same job id.
3. **Claim.** Before touching the instrument, the driver claims the job. The executor
   records the claim under the step's row lock and answers `go`. *API: `running`, no start
   time yet: permission is not evidence.*
4. **Run.** The instrument writes its ledger entry before any physical work. The driver
   reads the ledger, not `Run`'s return value (a hung job never returns), and publishes
   `started` and then `finished` with the ledger's times, outcome and data hash. *API:
   `running`, with the ledger's times.*
5. **Store and verify.** The driver uploads the bytes, retrying until storage accepts them
   and keeping the device busy meanwhile, then publishes `stored`. The executor reads the
   object back and compares its sha256 with the ledger's. *API: `completed`, with
   `data_ref`.*
6. **Finish.** When no step is in flight, the run ends. Its `finished_at` is the last ledger
   time, not the moment the executor noticed.

### Recovery

- **A driver restarts:** it first *replays* its ledger. It reports how every job ended, and a
  job cut off by the restart is reported `hung` (the instrument marks it so). For each
  completed read, it asks storage: present → `stored`; still absent after a 10s grace →
  `data_lost`.
- **A step is claimed but nothing says it started:** the executor keeps resending the same
  command every 3s (§2.3).
- **The executor restarts:** its intent is in Postgres and its facts wait in the stream, so
  nothing in its memory needs recovering.
- **A read was reported lost:** every 10s the executor looks in storage for its object (§2.4).

---

## 2. Why this topology

### 2.1 Measured before designing

Before writing code, I cut a bare NATS client off the network for 6s and 20s, with default
and with 2s pings (C1). My prediction was wrong twice (measured):

- **Requests that timed out were still delivered,** up to 20s late. With default pings the
  TCP connection survived, so nothing was lost, but everything was late.
- **7 of 8 JetStream publishes that reported a timeout had been stored.**

So **a timeout means "unknown", never "didn't happen"**. Any design that turns "no reply"
into "start a new attempt", or "failed", would sooner or later run a job twice or record a
false outcome. Every decision below follows from that.

### 2.2 Requests for commands, a stream for facts, Postgres decides

A lost command is harmless: it is sent again. A *stored* command is dangerous: it can run
late, after a cancel or after someone has moved the plate. Facts are the opposite: losing one
leaves the record wrong, so they must survive an executor that is down.

| Alternative | Why not |
| --- | --- |
| Requests only; the executor polls drivers for outcomes | While the executor is down, facts exist only in driver memory and the ledger. It also assumes an instrument can always be asked, which is true of this simulator, not of most equipment |
| JetStream for commands too | A durable command can run late. One ack timeout would also have to fit jobs of 2 seconds and of hours |
| Shared state in NATS KV | A second source of truth beside Postgres, without transactions |

**Trade-offs.** Delivery is at least once, not exactly once, so every fact must apply
idempotently, and it does. When the executor is cut off, events already handed to it are
redelivered only after the 30s ack wait. That delays the record, but doesn't falsify it
(§4).

### 2.3 Claim before run, and resend the same job

**Claim.** A late command, or a cancel arriving at any moment, must never lead to a job
starting after its cancel. The driver's claim and the cancel take the same row lock, so
exactly one of them is first:

- if the cancel is first, the claim gets `abort`;
- if the claim is first, the job may run, and its outcome is recorded when it arrives.

The rejected alternatives, commands that expire or a broadcast cancel, only narrow the
window. They still race with the cancel, and expiry depends on synchronised clocks.

**Resend.** My first version stopped sending once a step was claimed. That left a gap: a
driver that dies after its claim, before starting, leaves nothing in its ledger, so no event
ever comes. I reproduced it (PR #18, measured). I held the step's row lock in `psql`, so
`liquid-handler-1`'s claim for `fill_sample_plate` waited in the executor. Then I killed the
driver and released the lock, so the `go` went to a dead inbox. The run was still `running`
20s after the driver restarted.

The fix is to keep resending the same command while a step is claimed with no evidence, and
to treat the driver's answer as evidence:

- **`known`:** the driver has the job, in flight or in its ledger.
- **`accepted`:** the driver has no record of it, so it never started (the ledger is written
  before any physical work). It claims again, gets `go`, and runs it once.
- **No reply:** still unknown, so the step stays `running`.

With the fix, a fresh reproduction completed: the job started 1.4s after the driver
restarted, and the ledger has exactly one execution. I rejected a separate "what do you know
about this job?" query, because the command already asks exactly that.

**Trade-offs.** The claim costs one extra round trip per job. A driver that never comes back
leaves its step `running` for ever (§5). The guarantee rests on one driver per instrument,
with a ledger that survives restarts, and on one executor answering claims (assumed).

### 2.4 Data counts only once verified; lost data stays open

A read's bytes reach the driver only in memory, so the ledger saying `completed` is not
enough. A read completes only once its stored object matches the ledger's hash (§1). Once
verified, it stays `completed`: a later mismatch is reported in `error` instead, because steps
after it may already have used the data.

The harder case is a driver that crashes before its upload succeeds. The restarted driver
finds no object, and reports `data_lost`. My first version failed the read after a grace
period. On that version, the sequence below finished the run `failed`, and a late upload then
contradicted the record. Nothing bounds how late a dead process's upload can land (inferred:
neither S3 nor SeaweedFS promises it), so "absent now" is not "lost".

So a lost read is now *unresolved*. The API says `running`, and `error` says an earlier upload
may still appear. The plate reader reads the plate again at once, the executor checks storage
for the original every 10s, and whichever attempt is verified first completes the step.

C2 tested this rule (measured). I stopped storage mid-read, restarted the driver after the
read, started storage again, then put the read's bytes at its key by hand, standing in for a
late upload. The run stayed `running` until the object appeared, then completed within one
10s check.

Making loss final would need storage to refuse a late write. SeaweedFS 3.62 ignores
`If-None-Match: *`: a second conditional PUT returned 200 and overwrote the object
(measured). **Trade-off:** if every re-read also fails and the original never appears, the
run stays `running` (§5). C2 ran before re-reads existed, so that path is covered by unit
tests, not a live run.

### 2.5 Retry only with a positive reason

Without retries, every fault fails the run. A careless retry, though, could dispense liquid
twice into the same plate. So a new attempt needs a statement that repeating is safe, from
the right source for its cause:

| Cause | Safe to repeat if | Default |
| --- | --- | --- |
| The instrument failed | the instrument marks the failure `retryable` | liquid handler no; incubator and plate reader yes |
| The instrument hung | the device is in `REPEAT_SAFE` | plate reader only |
| A read's data was lost | the device is in `REREAD_ON_DATA_LOSS` | plate reader only |

- At most 3 attempts per step.
- **No timers:** a hang is retried only after the instrument itself reports `hung`, so a retry
  never goes to an instrument that is still frozen.
- **A step is decided by all its attempts:** `completed` once any attempt is verified;
  `failed` only when every attempt has ended and none will follow.

In C3 (measured) a hung plate read was repeated 31ms after the ledger reported the hang, and
completed. A hung `fill_sample_plate` on the liquid handler failed instead: "instrument hung;
outcome unknown; not repeated: liquid-handler-1 is not safe to repeat". A hung dispense may
have moved some, all or none of the liquid.

**Alternatives.** If an operator decided every retry, runs would never end by themselves
under faults. The instrument's flag alone can't decide a hang, because a hang has no flag.
Treating the incubator as safe to repeat would let runs finish, but only by incubating a
plate twice for an unknown total time.

**Trade-offs.** Which operations are safe to repeat on real equipment is a guess, so it is a
setting per device, not code. Retries have no backoff. A retried step is two jobs in its
ledger, so the acceptance script's check 5 ("no step ran more than once") fails on such runs,
by design.

### 2.6 Cancel stops what hasn't started, and keeps what has

`CancelRun` is one transaction. It locks every step of the run in a fixed order, so two such
transactions can't deadlock, then:

- a step sent but not claimed is cancelled, and a later claim for it gets `abort`;
- a step not yet sent is cancelled, and is never sent;
- a claimed or running step is **not** stopped (it may be moving liquid), and its outcome is
  still recorded;
- nothing is retried after the cancel.

Once nothing is in flight, the run is `completed` if every step completed, `failed` if a step
failed for its own reason, and `cancelled` otherwise. I rejected "cancelled whenever a cancel
was requested", because it would hide a failure that happened anyway.

**One hard case:** a claim granted to a driver that then died before starting. The resend
(§2.3) would run the job once the driver is back, even after a cancel. A driver claims only
before running a job its ledger doesn't have, so a second claim proves the job never
started. After a cancel, that second claim is therefore withdrawn and answered `abort`.
Measured live: the step ended `cancelled`, with nothing in the ledger. This relies on one
executor answering claims in order (§5).

---

## 3. GraphQL schema

`POST /graphql` on port 5002, alongside the unchanged REST API. This is the schema in
`services/executor/graphql.go` without its field descriptions. It is valid SDL, and parses to
the same types, fields and enum values:

```graphql
scalar Time

type Query {
  run(id: ID!): Run                  # null if there is no such run
  runs(status: RunStatus): [Run!]!   # newest first
  workflows: [Workflow!]!
}

type Mutation {
  createRun(workflowName: String): Run!
  startRun(id: ID!): Run!
  cancelRun(id: ID!): Run!           # stops what hasn't started
}

type Workflow { name: String!  isDefault: Boolean!  steps: [String!]! }

type Run {
  id: ID!  workflowName: String!  status: RunStatus!
  cancelRequested: Boolean!          # it may still be running: claimed work isn't stopped
  createdAt: Time!  startedAt: Time  finishedAt: Time
  steps: [Step!]!
}

type Step {
  name: String!  device: String!  dependsOn: [String!]!
  status: StepStatus!                # exactly as REST, like the times, error and dataRef
  phase: StepPhase!                  # finer than status
  startedAt: Time  finishedAt: Time  error: String  dataRef: String
  attempts: [Attempt!]!              # every attempt, first first
}

type Attempt {
  number: Int!  jobId: ID!  phase: AttemptPhase!
  decided: Boolean!  claimed: Boolean!
  outcome: Outcome                   # null until the ledger says
  startedAt: Time  finishedAt: Time  # this attempt's ledger times
  instrumentError: String  retryable: Boolean!  unresolved: String
  data: AttemptData                  # reads only
}

type AttemptData { ledgerSha256: String  verified: Boolean!  dataLost: Boolean!  checks: [StorageCheck!]! }
type StorageCheck { ref: String!  sha256: String!  matches: Boolean!  at: Time! }  # at: the executor's clock

enum RunStatus    { PENDING RUNNING COMPLETED FAILED CANCELLED }
enum StepStatus   { PENDING RUNNING COMPLETED FAILED CANCELLED }
enum StepPhase    { PENDING DISPATCHED RETRYING CLAIMED RUNNING STORING UNRESOLVED ENDED COMPLETED FAILED CANCELLED }
enum AttemptPhase { DISPATCHED CLAIMED RUNNING STORING UNRESOLVED COMPLETED FAILED CANCELLED }
enum Outcome      { COMPLETED FAILED HUNG }
```

Errors carry `extensions.code`: `NOT_FOUND`, `BAD_USER_INPUT`, `WORKFLOWS_INVALID` or
`INTERNAL`.

**Why this shape:**

- **It can't contradict REST.** It has no state of its own. Steps are the same rows REST
  reads, and a run and its steps are read in one repeatable-read snapshot. Mutations call the
  same `Executor` methods.
- **It adds what REST's fixed shape can't show:**
  - Every attempt, with its own ledger times. After a retry, REST shows only the attempt that
    decided the step, so in C3 a 30s hung read on the plate reader was invisible. Measured:
    three hung attempts in GraphQL matched three ledger executions.
  - For a read, the ledger's hash and every storage read-back, with `matches`.
  - The internal `phase`, which tells "sent" (a cancel still stops it) from "claimed" (it
    won't).
- **Small, and polling:** three queries and three mutations, enough to start runs and follow
  them. Subscriptions would need a websocket transport and a change feed the executor doesn't
  have, and runs take seconds to minutes.
- **`graph-gophers/graphql-go`,** schema-first, so the schema is the design and resolvers are
  plain struct fields. gqlgen would add a code generator and thousands of generated lines.

---

## 4. What I did to break it

Each experiment: write the prediction down, inject the fault, compare the record with every
ledger and with storage, and log where the prediction was wrong. From C4 on,
`scripts/check-run.py` does the comparison: outcomes and times against the ledgers, every
ledger execution present in the record, no job run twice, and every read's object matching
its hash. I tested the checker against six kinds of doctored ledger; it caught all six.

The instrument picks faults from `sha256(seed:device:step:attempt)`, with no run id. So
under one seed every run meets the same faults, and only a retry rolls again (measured). I
used this to choose seeds that exercise a given path.

| | Fault | Result | What changed |
| --- | --- | --- | --- |
| C1 | NATS client cut off, 6s and 20s, default and 2s pings | Timeouts don't mean "not delivered" | The messaging design (§2.1–2.3) |
| C2 | Storage stopped mid-read, driver restarted before upload, late upload by hand | The read stayed open and completed once the object appeared. On the first version the same sequence failed the run, and the upload then contradicted it | Lost data stays open (§2.4) |
| C3 | 30% hangs on every device, seeds 0 and 17 | Every prediction held; all records true | GraphQL shows every attempt (§3) |
| C4 | 26 runs: executor and drivers killed mid-step or idle; executor and plate reader cut off; storage down 15s; `chaos.sh random` with seeds 7 and 11; instrument faults at seeds 3, 13, 29 | **All 26 records true** (`check-run.py`: 0 problems). A driver killed mid-job is marked `hung` at its restart: the plate reader retried it, the liquid handler failed the step with its reason | One latency issue logged, not fixed (below) |

To repeat C4: `scripts/experiments/c4-fault.sh <a-h>` (one fault per run) and
`FOR=90 scripts/experiments/c4-random.sh <seed> <out>` (random chaos, runs back to back),
with instrument faults set by, for example,
`CHAOS_SEED=13 INC_FAIL_PCT=20 PR_HANG_PCT=30 HANG_DURATION=5s docker compose up -d liquid-handler-1 incubator-1 plate-reader-1`.
C1's probe was a local script, not committed, so C1 can't be fully rerun from the repo.

**Where C4 proved me wrong: time, never truth.**

- **An 8s executor cut delayed the run by 30s.** The run took 41s, against about 11s
  predicted. Watching the consumer showed two events "delivered, not acked" for exactly the
  30s ack wait. They had been handed to the executor's pull request as it was cut off. The
  record said `running` for steps that had ended: behind, not false. A shorter ack wait would
  reduce this.
- **The storage outage cost more than its length.** Storage was down 15s, but the read
  completed 17s after storage was started again. SeaweedFS logged "wait to connect to filer"
  for about 16s, and the driver's upload succeeded on its next retry. I infer that shorter
  outages pay the same restart cost, but I measured only this one.

**Other checks on every change:** a check written first; mutation checks (deliberately broken
code, to confirm the tests catch it); `go test -race ./...`; and `acceptance.sh`, which passes
7/7 on both workflows with faults off.

---

## 5. What is still not true, or not proven

None of these makes the record false. Detail and possible fixes are in
[`docs/forengineer/concerns.md`](docs/forengineer/concerns.md).

**Boundaries I drew on purpose:**

- **A run can stay `running` for ever** when the outcome is truly unknown: a driver that
  never comes back, or a lost read whose re-reads all fail while the original never appears.
  The fix is an operator action that marks the step `failed` with the reason. It was designed
  but not built.
- **Work that has started is never stopped,** by a cancel or a timer.

**Assumptions:**

- **One driver per instrument, with a ledger that survives restarts.** "A resend never runs a
  job twice" depends on both.
- **One executor.** With two, a stalled first claim could look like a re-claim after a cancel
  and withdraw a running attempt. Numbered claims would fix it.
- **The instrument's end-of-job ledger write succeeds.** If it failed, a restart would report
  `hung` for a job we recorded as completed (`instrument.go:241`, inferred).
- **One NATS server,** no retention limit on `LAB_EVENTS`, and no rolling upgrades.

**Known gaps:**

- Delivery is at least once, and the record can lag: up to 30s after an executor network cut,
  and by storage's own restart time after an outage.
- REST `GET /runs/{id}` isn't read from one snapshot, so for one poll a run can look `running`
  beside steps that have all ended. GraphQL is.
- `createRun` isn't idempotent: a client retrying after a lost reply creates two runs.
- Retries have no backoff, and acceptance check 5 fails on retried runs.

**Not proven:**

- The re-read after lost data was not run live, only in unit tests.
- Several runs competing for one device: chaos ran runs back to back, not concurrently.
- Scale beyond three devices: the dispatch loop rescans running runs every 500ms, and replay
  resends a driver's whole ledger on every start.

---

## 6. How long I spent

The project took roughly 10 hours overall, including AI-assisted implementation, testing and review cycles.

My own active time was around 4–5 hours, mainly spent on architecture, evaluating design alternatives, making decisions, reviewing results, investigating failures and refining the AI development workflow.

---

## What I thought of the exercise

I enjoyed the exercise. It reminded me of some of my earlier work on industrial control systems and communication protocols, where you have to be careful about what happens when software interacts with physical equipment. A timeout doesn't mean an operation didn't happen, and getting that wrong can have real consequences.

One thing I found interesting was how much the way I worked with AI affected the result. At the beginning, I moved a bit too quickly and followed an architecture suggested by AI. After thinking through some failure scenarios, I realised parts of it didn't really make sense. I went back, explored the alternatives more carefully, and started using interactive HTML diagrams to help discuss and understand the design. That was useful, especially for getting the core architecture right before building more on top of it.

I also spent time setting up and refining the project-specific skills and development workflow. I think this is an important part of working with AI, not just something to do once at the start. On a real project, I'd spend more time improving these workflows as the team learns, capturing what works and making it reusable for others. A good setup helps keep the work consistent, but it also needs to evolve.

AI can take on a lot of the implementation, testing and documentation work. But I still think the important design decisions need to be owned by engineers. It's easy to accept something that looks reasonable at first, so being willing to question it, test it and change direction matters.

I particularly liked the chaos tools and instrument ledgers. They gave me a way to check whether the system actually behaved as expected, rather than just trusting the code or the explanation. A few experiments proved my initial assumptions wrong, which was probably the most useful part of the exercise.

Overall, I liked that the exercise gave me room to explore different approaches, make trade-offs and change my mind based on evidence.

---

## AI use

### How we set up the work

Before writing any code, we spent time on `CLAUDE.md` and three project skills in
`.claude/skills/`. With an AI writing most of the code, the risk is code that looks right and
makes claims nobody has checked. That is the same failure the run record must avoid. So
`CLAUDE.md` states the invariant (the record must be true, the ledger is the evidence) and a
review checklist for every message and state change: what if it arrives twice, late or never,
or the process restarts halfway? Each skill makes one kind of work produce evidence:

- **`/decide`** was used for the two biggest forks, messaging and retries. It set out two or
  three real alternatives and the assumption each rests on, and settled facts by experiment
  where it could, before I chose. C1 came before the messaging design, and measuring that the
  fault roll ignores the run id came before the retry design. Smaller forks, such as cancel and
  GraphQL, were argued the same way inside their slices. All are in the decisions log.
- **`/slice`** kept each change to one behaviour and one PR into `dev`. Each started from a
  check that failed first, passed `go test -race` and the acceptance script, and was reviewed
  once by Codex. It ended with an explanation I could check before merging. Reviewing those
  explanations and decision records is where the reversals below came from.
- **`/break`** made each experiment start with a written prediction, and end with the record
  compared against the instrument ledgers and storage, not the API's word. That comparison
  became `scripts/check-run.py`. Wrong predictions were logged as findings, such as the 30s
  ack wait in §4.

None of this was about generating code faster. It kept each claim, in the code and in these
notes, tied to evidence.

### Who did what

- **Claude Code** wrote the code, tests, scripts and documentation, including this note and
  the HTML explainers. It followed `CLAUDE.md` and the `/decide`, `/slice` and `/break`
  skills in `.claude/skills/`.
- **I made the design decisions, and reviewed and merged every PR.** Each design fork was
  argued with its alternatives before I chose. Several first versions were changed in my
  review:
  - lost data was failed after a grace period;
  - one retry trigger covered every cause, with the incubator treated as safe to repeat;
  - a run counted as cancelled even when a step had failed on its own.
- **Codex** reviewed each slice PR once, read-only (`scripts/codex-review.sh`). Its findings,
  and how each was handled, are in the PR descriptions. One changed the cancel design: a
  cancel could relabel a failure that was final anyway as "not retried: cancelled".
- **The conversations are in `ai/`.**
  <!-- TODO(user): export, review for anything private, and say here if anything was left out. -->
