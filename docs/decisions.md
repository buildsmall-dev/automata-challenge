# Decisions

Design decisions, newest last. Each one feeds NOTES.md §1–3.

## Time log

| Session | Start | End | Notes |
| --- | --- | --- | --- |
| 1 | 2026-10-25 | 2026-10-26 | exploration, repo setup, working process, D1 + disconnect experiment |

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
  - **Commands:** core request `lab.<device>.cmd.run {v, job_id, run_id, step, attempt}` →
    `accepted | busy | known`. The queue lives in Postgres; the executor sends only to a
    device it believes free. A timeout means *unknown*: resend the same `job_id`, never a new
    attempt, don't treat the device as free.
  - **Claim before run:** before `inst.Run` the driver requests `lab.executor.claim {job_id}`.
    One conditional update (`phase dispatched → claimed`, run not cancelled) on the same row
    `CancelRun` changes, so exactly one of claim and cancel wins. `go` runs; `abort` or a
    timeout does not run (retry the claim, it is idempotent). Replaces command expiry.
  - **Events:** driver publishes `accepted / started / measured / finished` to JetStream stream
    `LAB_EVENTS` (`lab.<device>.evt.*`), `Nats-Msg-Id` = job_id + kind, retried until PubAck.
    Executor: durable pull consumer, idempotent guarded apply, ack after DB commit. The
    executor provisions the stream at startup.
  - **Reconcile:** `lab.<device>.job` request/reply answered from ledger + storage, used on
    executor startup and for in-flight steps past their deadline (R2). Drivers replay recent
    ledger outcomes as events on startup (R1).
  - **Truth rules:** unknown stays unknown (a claimed step whose driver is unreachable stays
    `running` with "outcome unknown", never failed on a timer). Late facts always win, even
    after a cancel. A read is `completed` only when the ledger's sha256 equals the hash of the
    object at `runs/<run>/<step>/<job_id>.csv`, checked by the executor. Data lost before
    storage is a failed attempt, retried.
  - **Internal phases:** `dispatched` (API pending) → `claimed / running / storing / unknown`
    (API running) → `completed | failed | cancelled`. A dispatched step occupies its device.
  - Set NATS `PingInterval` explicitly to a few seconds.
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
- **Known limits:** plate data lost in the crash window before upload costs a re-read (a
  second ledger execution); a driver that never returns leaves a step unknown (operator
  "declare lost" deferred); dedupe window 2 min; single NATS server and single executor.
  Explainer: `tmp/d1-messaging-v2.html`.
- **Slice / PR:** this PR records it; implementation slices follow.

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
  instead of overloading `error`).
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
