# Decisions

The working log, written during the work. The readable version, current state only, is
[`../forengineer/decisions.md`](../forengineer/decisions.md); update it when this changes.

Design decisions, newest last. Each one feeds NOTES.md §1–3.

## Time log

| Session | Start | End | Notes |
| --- | --- | --- | --- |
| 1 | 2026-10-25 | 2026-10-26 | exploration, repo setup, working process, D1 + disconnect experiment |

Later sessions, and the total time spent (NOTES.md §6), are for the user to fill in. Commit
times are not used for this: they are far from the actual active time.

## Template

```
## D<n>. <decision, as a statement>

- **Question:** what we had to decide, and why it matters for the record being true
- **Chosen:** what we do
- **Alternatives:** what else we considered, and why not
- **Rests on:** the assumption it depends on, and the evidence (measured / inferred)
- **Would revisit if:** what would change our mind
- **Slice / PR:** where it was built
```

---

## D1. Commands by request/reply with claim-before-run; facts through JetStream; Postgres decides

- **Question:** how commands reach the drivers and outcomes get back, so the record stays
  true when messages are lost, duplicated or late, and processes or networks drop. Judged
  on production reliability, scalability, maintainability and operational cost.
- **Chosen:** option C, the hybrid. Rule: durable facts, short-lived intentions, Postgres decides.
  Wire contract in `services/common/protocol`.
  - **Commands:** core request `lab.<device>.cmd.run {v, job_id, run_id, step, attempt}` →
    `accepted | busy | known`. The queue lives in Postgres; the executor dispatches what
    `NextSteps` says is ready in each run (a dispatched step occupies its device) and sends
    the command until there is evidence the job started (claimed is not enough, S7): again
    after 500ms on busy or no answer, after 3s on accepted or known. A timeout means *unknown*: resend the same `job_id`, never a new
    attempt, don't treat the device as free. Runs sharing a device are not queued against
    each other; they retry on `busy` (K29).
  - **Claim before run:** before `inst.Run` the driver requests `lab.executor.claim {job_id,
    device}`. The executor applies a `Claimed` fact under the step's row lock, the same lock
    a cancel takes, so exactly one of claim and cancel wins. Only a claim from the step's
    assigned device counts. `go` runs; `abort` does not; no answer (the executor does not
    reply when the database fails) does not run and the driver asks again (claims are
    idempotent). Replaces command expiry.
  - **Events:** the driver publishes `started / finished / stored / data_lost` to JetStream
    stream `LAB_EVENTS` (`lab.<device>.evt.<kind>`), `Nats-Msg-Id` = `job_id:kind`, retried
    until PubAck. Times and outcomes come from the ledger. The executor provisions the
    stream at startup and reads it with a durable pull consumer: decode and validate (Term
    if malformed, or if its device is not the one in its subject), apply idempotently under
    the row lock, ack only after the commit; an event from a device the step is not
    assigned to, or for a job no step has, is acked and logged; a database error is Nak'd.
  - **Plate data:** the driver uploads a completed read's bytes to `runs/<job_id>.csv`
    (`runs/<run>/<step>/<attempt>.csv`), retrying until storage takes them and holding the
    device meanwhile, then publishes `stored` with the ref. The executor does not take the
    ref on trust: it reads the object at the key it derives itself and hashes it.
  - **Replay (R1):** on start the driver reports, from its ledger as it was before it
    listened for commands, how every job ended (`started`, `finished`; a job cut off by
    the restart is `hung`), then for each completed read asks storage: present → `stored`;
    absent after an upload grace (2 × the storage timeout) → `data_lost`; no answer → ask
    again.
  - **Reconcile (R2, S7): by resending, not by a query.** A step claimed with no evidence
    (the driver died after its claim, or its instrument refused the job, K19, K25) is sent
    the same command, with the same `job_id`, every 3s until `started` or another fact
    arrives. The command is the question:
    - `known`: the driver has the job in flight or in its ledger, which the instrument
      writes before anything physical happens (`instrument.go`), so nothing more is done.
    - `accepted`: the driver has no record of the job, so it never started. The driver
      claims it again; the attempt is already claimed, so the executor answers `go`, and it
      runs once.
    - No answer: unknown stays unknown, so the step stays `running` and the command is asked
      again.

    A resend is never a new attempt. Because it is driven from Postgres, it also covers an
    executor restart.
  - **Truth rules:** unknown stays unknown (a claimed step whose driver is unreachable stays
    `running`, never failed on a timer). Late facts always win, even after a cancel. A read
    is `completed` only when the ledger's sha256 equals the hash of the object at its key,
    read back by the executor; once verified it stays completed (D2). A read reported lost
    is **unresolved**, not failed, and its run stays open: an earlier upload may still land,
    and the executor looks for it every 10s (D3). Ending it is a decision (retry policy or
    operator), not a timeout.
  - **Internal phases:** `pending`, `dispatched` (API pending) → `claimed / running /
    storing / unresolved` (API running; `unresolved` carries its reason in `error`) →
    `completed | failed | cancelled`. A run finishes only when no step is in flight:
    completed if all completed, failed if one failed, `finished_at` the last ledger time.
  - NATS `PingInterval` 5s, `MaxPingsOutstanding` 2, in the executor and the drivers.
- **Alternatives:**
  - *A, request/reply + executor polling:* simplest, and recovery is the main path, but facts
    live only in driver memory and the ledger while the executor is down, and it assumes we
    can always ask the instrument: true of this simulator, false of most equipment.
  - *B, JetStream both ways:* durable command queue makes stale physical commands possible
    (late runs after a cancel or a hand-moved plate) and puts scheduling in a FIFO; AckWait
    tuning against 2s–hours jobs and 30s hangs.
  - *D, shared state in NATS KV:* a second source of truth beside Postgres, no transactions.
  - *Command expiry / cancel broadcast* instead of a claim: both narrow the late-command
    window but race with the cancel; expiry also depends on clocks.
- **Rests on:** the ledger is written before work starts and set to completed before `Run`
  returns (`instrument.go:166-170, 226-228`) — inferred. Timeouts don't mean "not
  delivered" and failed JetStream publishes may be stored — measured, C1.
- **Would revisit if:** many runs must queue per device with the queue as source of truth
  (B with a claim); the extra claim round trip proves too slow; we need executor replicas
  (per-run locking).
- **Known limits:** a read whose data is really gone stays unresolved, and its run open,
  until a retry policy or an operator ends it (D3, K39; no automatic re-read yet); a driver
  that never returns leaves a step unknown (operator "declare lost" deferred, K6); dedupe
  window 2 min, after which repeats rely on idempotent apply; replay covers the whole
  ledger on every start (K35); single NATS server and single executor.
  Explainer: `docs/html/d1-messaging-v2.html` (as recorded; not updated for the changes below).
- **Slice / PR:** recorded in #3. Built in S1 #4 (lifecycle), S2 #5 (store), S3 #6
  (contract), S4 #7 (driver), S5a #8 (executor receives), S5b #9 (executor sends),
  S6 #10 (plate data), R1 #11 (replay).
- **Changes since recorded** (2026-10-26, to match the implementation):
  - Event kinds were `accepted / started / measured / finished`; now `started / finished /
    stored / data_lost` (S3, S6). `accepted` is the command reply; nothing is "measured"
    separately from `finished`.
  - Data key was `runs/<run>/<step>/<job_id>.csv`; now `runs/<job_id>.csv` (S3 `DataKey`),
    the same path without repeating run and step.
  - "Data lost before storage is a failed attempt, retried" → unresolved until storage shows
    the data or someone decides (D3). Nothing retries automatically yet.
  - Phase `unknown` is called `unresolved`, with a reason (S1).
  - The claim was "a conditional update `dispatched → claimed`"; it is the `Claimed` fact
    applied under the row lock (S2), accepted only from the step's assigned device, with no
    reply on a database error (S5a, Codex).
  - R1 replays the whole ledger as it was before listening, not "recent" outcomes, and
    checks storage for each completed read (R1).
  - Events whose device does not match their subject, or the step's assigned device, are
    dropped (S5a, K15).
  - (2026-10-27) R2 was a `lab.<device>.job` query; it is a resend of the same command while
    a step is claimed with no evidence (S7). The command already asks the driver what it
    knows of the job, and its idempotence (`known`) is what makes the resend safe, so a
    query would add a subject and driver code to learn nothing more.
  - (Review 2026-10-27) Superseded lines above:
    - "Nothing retries automatically yet" and, in Known limits, "no automatic re-read yet":
      D4 now retries (PRs #13–#16). A lost read is re-read at once on a device with
      `reread_on_data_loss`.
    - Internal phases also include `retrying` and `ended` (D4).
    - "Failed if one failed" is now D5's rule 1: `failed` only if a step failed for its own
      reason, `cancelled` if the cancel is why the rest didn't happen.
    - "Single executor" is now also an assumption the withdrawn claim rests on (K56).

## D2. A verified completion is never undone; later storage problems are reported

- **Question:** a read is completed once a read-back of storage matches the ledger's sha256.
  If a later read-back does not match, should the step go back to unresolved?
- **Chosen:** no. "Completed, data verified at T" stays true forever, so the status stays
  `completed` and `data_ref` stays set. The newest mismatching read-back is reported in
  `error` ("stored data no longer matches what the instrument measured (checked T)") and
  cleared by a newer matching one. Before verification, a mismatch leaves the step
  unresolved; with `DataLost` (the driver no longer has the bytes) it was failed, until
  D3 changed that to unresolved.
- **Alternatives:** move the step back to unresolved (API running): the record stays true
  but a status regresses, and dependents may already have run. Ignore checks after
  completion: hides corruption.
- **Rests on:** evidence is a set of read-backs, so "some check matched" does not depend on
  arrival order (tested over all orderings).
- **Would revisit if:** we need data integrity as its own field (GraphQL could expose it
  instead of overloading `error`). (Review 2026-10-27: done in D6. `Attempt.data` has
  `verified` and every check with `matches`; REST's `error` is unchanged.)
- **Slice / PR:** S1, step lifecycle.

## D3. A read reported lost stays unresolved; only storage, or a decision, settles it

- **Question:** a restarted driver reports `data_lost` when storage has no object for a
  read its ledger completed. Is that enough to fail the read, and finish the run as failed?
- **Chosen:** no. `DataLost` makes the read **unresolved** (API `running`, `error` says the
  data was lost but an earlier upload may still appear), so its run is never finished while
  the outcome is open. The executor looks in storage for each such read every 10s
  (`recheckLoop`); a matching object completes it (late facts win), as if `stored` had
  arrived. Ending it any other way is an explicit decision, not a timeout: re-run the read
  (hang/retry `/decide`) or an operator declares it lost (K6).
- **Alternatives:** fail after a grace period (R1 first did this, with 2 x the storage
  timeout): a delayed upload can still land later, and then the record contradicts
  storage (user review of R1). Let storage arbitrate with create-only writes
  (`If-None-Match: *`), tombstone the key before declaring loss: would make "lost" a fact,
  but **measured**: SeaweedFS 3.62 ignores the header (a second conditional PUT returned
  200 and overwrote). Fence late uploads with a per-process key: the data would still be in
  storage, so "lost" would still be false.
- **Rests on:** no bound on when a dead client's upload can land (**inferred**: nothing in
  S3 or SeaweedFS promises one). The driver keeps its grace period only to avoid a needless
  `data_lost`, not for correctness.
- **Would revisit if:** storage supports conditional writes (newer SeaweedFS, real S3:
  then tombstone-and-arbitrate, and lost becomes final); or runs must end on their own
  (then a retry policy ends them, not a guess).
- **Slice / PR:** R1 (PR #11), after review.
- **Review 2026-10-27:**
  - Of the two decisions named in Chosen, the re-read was built (D4, PR #16). The operator
    one was not (K6, K39).
  - C2, the experiment behind D3, ran before D4. Today the same sequence also starts a
    re-read at once (unit test `TestLostReadIsReadAgain`; not re-run live).

## D4. Retry per cause and per device, bounded; a step is decided by all its attempts

- **Question:** what ends a step that cannot succeed as it stands (a retryable failure, a hang,
  a read whose data was lost), and when to start a new attempt. Today a hung or lost step stays
  `running` forever, and a retryable failure fails the run. A careless retry can repeat liquid
  handling physically.
- **Chosen:** option B, automatic and bounded, with a trigger per cause:
  - **A new attempt needs a positive claim that repeating is safe, from the source for its
    cause; no claim, no retry.** The sources: the instrument's `Retryable` (failure), the
    device's `repeat_safe` (hang), the device's `reread_on_data_loss` (data lost).
  - **Retryable failure:** a new attempt at once. The driver forwards the instrument's
    `Retryable` in `finished` (it is not in the ledger). A `finished(failed)` without the
    flag, as replay sends after a driver restart (R1), is not retried: the step is `failed`.
    So the same physical failure can end differently around a restart, always on the side of
    not repeating.
  - **Hung:** a new attempt after `finished(hung)`, only on a `repeat_safe` device. Otherwise
    the step is `failed`: "instrument hung; outcome unknown; not repeated". There is no timer:
    the retry waits for the instrument's own report that it gave up, so it never goes to a
    frozen instrument.
  - **Data lost:** a re-read at once, alongside the still-open attempt (D3), on a device with
    `reread_on_data_loss`. Don't wait for the late upload; D3 found no limit on that wait.
  - At most 3 attempts per step, shared by all causes.
  - **Cancel, as D1 has it, applied to attempts** (review 2026-10-27: two points below are
    refined by D5. "Never claimed" became "never ran": a claim withdrawn after a cancel
    doesn't count (K54). And "not retried: cancelled" is the reason only if the policy would
    have retried; otherwise the failure keeps its own reason):
    - A cancel is our intent on the step, recorded whatever state the step is in: no new
      attempt is dispatched after it. Retry and cancel both take the step's row lock, so
      one of them is first, as claim and cancel are in D1.
    - An attempt dispatched but not claimed is cancelled; exactly one of claim and cancel
      wins (D1). An attempt already claimed is not stopped: it may be running, so only its
      evidence says how it ended, and its late facts are still recorded (D1, "late facts
      always win").
    - The step is `cancelled` only if no attempt was ever claimed. Otherwise it is decided
      by its attempts, as below, with the cancel being one reason no further attempt will
      be made (`failed`: "…; not retried: cancelled"). A cancel never hides a physical
      attempt, and never undoes a completion (D2).
    - A read whose data was lost when the cancel came is not re-read. Its attempt stays
      open until its upload appears (D3).
  - **A step is decided by all its attempts:** `completed` once any attempt is verified
    complete (the first verified one supplies `data_ref` and the times, D2); `failed` only when
    every attempt is finished, none completed, and no further attempt will be made; otherwise
    `running`. An attempt that is still open does not hold its run back once another attempt
    of the same step has completed.
  - Every attempt is kept (fixes K2). Facts for older attempts find their step by parsing the
    job id (`run/step/attempt`). REST shows the attempt that decided the step, or the latest
    if none has; GraphQL shows the history.
  - **Defaults:** repeat-safe = plate reader only; re-read on data loss = plate reader;
    3 attempts in total. The incubator is not repeat-safe: a hung incubation ran for an
    unknown part of its time, and repeating it adds an unknown amount of incubation to the
    same plate, whereas a second read leaves the plate unchanged. The incubator still retries a
    failure the instrument marks retryable. Cost (measured roll): at `INC_HANG_PCT=30`,
    seed 0, Triple Assay's `incubate` hangs on attempt 1, so every such run ends `failed`
    ("outcome unknown; not repeated"), the same trade as the liquid handler.
  - **Deferred:** an operator `abandonStep(run, step)` (GraphQL) for the one case the policy
    leaves open: a lost read whose re-reads also failed or lost their data stays `running`
    (K39). It would set `failed`, "abandoned by operator; a late upload may still appear",
    and only while the step is `running` with no verified attempt (D2); a late upload is still
    recorded in the attempt history. Recorded here, built with GraphQL if time allows.
- **Alternatives:**
  - *A, an operator decides (GraphQL `retryStep` / `abandonStep`):* never repeats anything by
    itself, but under any fault seed runs never end on their own (measured, below).
  - *C, the instrument's `Retryable` flag only:* a hang returns no flag, so C can't decide the
    hardest case. It becomes A for hangs, or an unsafe B (repeated dispensing).
  - *First draft of B (explainer v1):* one trigger for all three causes, and "a step is its
    latest attempt". Wrong for lost data: the first attempt is still open, so a failing re-read
    would have marked the step `failed` while its upload could still land (user review).
  - *A final hang left `running` until an operator acts:* `running` claims something is still
    happening after the instrument has stopped. CLAUDE.md: `failed` with the reason once we
    can't find out.
  - *Incubator repeat-safe (first recording):* Triple Assay would finish under hang faults,
    but by incubating a plate twice for an unknown total time (user review).
  - *A cancelled step shown `cancelled` whatever its attempts did:* would hide a physical
    attempt that ran, against D1's "a cancel stops what has not started".
- **Rests on:**
  - **measured:** the fault roll is `sha256(seed:device:step:attempt)`, with no run id, so
    under one seed a step's first attempt has the same fate in every run; only a retry rolls
    again (`incubate` hangs on attempts 1 and 2 at 30%, seed 0).
  - **inferred:** after the ledger says `hung`, nothing more is learned
    (`instrument.go:192-199`).
  - **inferred:** `FAILURES_RETRYABLE` is false for the liquid handler, true for the others.
  - **inferred:** `Retryable` exists only in `Run`'s return (`instrument.go:211`); the ledger,
    which replay reads, does not have it.
  - **inferred:** a hung job is reported once the instrument frees itself after
    `HANG_DURATION`, because the driver reads the ledger, not `Run`'s return
    (`instrument.go:198-203`, `adapter.go:41`).
  - **guess:** which operations are safe to repeat on real equipment, and whether a later
    re-read is an acceptable substitute. Hence both are settings per device, not code.
- **Would revisit if:** repeat-safety can't be stated honestly per device (then A); acceptance
  check 5 must hold with faults on (then A); storage gains conditional writes (a lost read
  becomes final under D3, and is retried like a hang).
- **Known limits:** a step whose re-reads all fail after a data loss stays `running` (K39,
  narrower; operator abandon deferred, above); a retry fails acceptance check 5 under faults
  (K5); a re-read is a different measurement, taken later; what a run's own status is after a
  cancel is left to the cancel slice (K42; review 2026-10-27: settled by D5).
  Explainers: `docs/html/d4-hang-retry.html` (v1), `docs/html/d4-hang-retry-v2.html`.
- **Slice / PR:** recorded in #12. Built in #13 (protocol `retryable`), #14 (lifecycle:
  attempts, the all-attempts rule), #15 (store lookup by job id) and #16 (policy). Tested by
  C3 (#17).
- **Review 2026-10-27:** the deferred `abandonStep` was not built, so K39 stays open. GraphQL
  (D6) has no mutation for it.

## D5. Cancel: stop what has not started, keep the evidence of what has; the run says why it stopped

- **Question:** what `CancelRun` does to each step, and what a cancelled run's status is once
  in-flight work ends (K42), including a claim that never started (K54).
- **Chosen:**
  - **One transaction for the whole cancel.** Every step of the run is locked in `position`
    order (K12), and `Cancel` is applied to each, so the cancel is never half applied.
  - **What a cancel does to each step:**
    - not sent → never will be;
    - sent, not claimed → cancelled, and exactly one of claim and cancel wins (D1);
    - claimed or running → not stopped, and its late facts are recorded (D1);
    - no new attempt after the cancel (D4); a lost read is not re-read and stays open (D3).
  - **A run not started** is cancelled at once. **A finished run** is left as it is.
  - **Withdrawn claim (K54):**
    - When a claim already granted is asked for again after a cancel, and the attempt has no
      evidence, the claim is withdrawn and the driver is told `abort`. The step can then end
      `cancelled`.
    - A driver asks for a claim only before it runs a job, and only for one its ledger doesn't
      have, so the request itself proves the job never started.
    - S7's resend is what asks again.
    - It rests on one driver process per instrument (K55), and on one executor answering a
      driver's claims in order (K56).
  - **The run's final status (rule 1),** once nothing is in flight:
    - `completed` if every step completed (the cancel came too late to stop any);
    - `failed` if a step failed on its own (the policy gave up, whether or not there was a
      cancel);
    - `cancelled` otherwise, meaning the cancel is why the rest didn't happen.
    - A step whose retry the cancel prevented is `failed`, "…; not retried: cancelled", and
      counts towards `cancelled`, not `failed`.
    - **The cancel does not decide an ended step; the policy does** (Codex, cancel slice). It
      names the cancel only if it would have made another attempt. A failure that was final
      anyway (not retryable, not safe to repeat, attempts used up) keeps its own reason, so
      the run is `failed`.
- **Alternatives:**
  - *Rule 2, `cancelled` whenever a cancel was requested and not every step completed:*
    rejected, because it hides a failure that happened on its own (user review).
  - *Stop resending a claimed step once it's cancelled:* rejected. A step whose driver died
    would then stay `running` for ever, because the resend is the only way to learn it
    never started.
  - *A run-level `cancel_requested` column:* rejected, because the steps' `Cancelled`
    already records it, with no schema change.
- **Rests on:** the ledger is written before anything physical happens, and the driver claims
  only before running (inferred: `instrument.go`, `adapter.go` `accept`/`claim`); measured
  live in the S7/cancel PR.
- **Known limits:** work already started is not stopped, by design. A cancelled run whose
  lost read never reappears stays `running` (K39; operator abandon is deferred).
- **Slice / PR:** #19 (cancel slice).

## D6. GraphQL is a second view of the same rows: the REST record plus every attempt

- **Question:** what `/graphql` exposes and where its answers come from. A second API that
  kept its own state, or derived the record differently, could contradict REST.
- **Chosen:**
  - **No state of its own.**
    - A step's fields are the columns REST reads.
    - Its `attempts` are the lifecycle JSON in the same row.
    - The run and all its steps are read in one repeatable-read, read-only transaction, so
      they are seen as of one moment.
    - Mutations call the `Executor` methods, then read the record again.
  - **What it adds to REST:**
    - `phase` (dispatched, claimed, storing, unresolved, ended…);
    - `cancelRequested`;
    - every attempt, with its own ledger times, outcome, retryable flag and why it is
      unresolved (K53);
    - for reads, the ledger digest and every storage read-back, with `matches` and
      `verified` (K1).
  - **Operations:**
    - `run(id)` (null if unknown), `runs(status)`, `workflows`;
    - `createRun`, `startRun`, `cancelRun`;
    - errors carry `extensions.code`: `NOT_FOUND`, `BAD_USER_INPUT`, `WORKFLOWS_INVALID`,
      `INTERNAL` (detail only in the log, as in REST).
  - **Library:** `graph-gophers/graphql-go`, schema first. The SDL in `graphql.go` is the
    design, and resolvers are struct fields.
  - **Following a run:** tools poll `run(id)`.
- **Alternatives:**
  - *gqlgen:* type-safe, but adds a generator and thousands of generated lines (user's choice).
  - *Subscriptions:* need a websocket transport and a change feed the executor doesn't have
    (the dispatch loop is poked in-process). Polling a snapshot is enough for a lab run that
    takes seconds to minutes (user's choice).
  - *Attempts as their own table:* rejected, because the lifecycle already holds them and a
    copy could drift.
- **Rests on:**
  - `saveStep` writes the columns and the lifecycle in one `UPDATE` (inferred, `store.go`).
  - Each attempt's times are the ledger's, carried by the events. Measured live: three hung
    plate-reader attempts match the three ledger executions, while REST shows only the last.
- **Would revisit if:** tools need push updates, or runs grow into the thousands (`runs` has
  no pagination, K60).
- **Slice / PR:** #20 (GraphQL slice).
