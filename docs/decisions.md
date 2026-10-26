# Decisions

Design decisions, newest last. Each one feeds NOTES.md §1–3.

## Time log

| Session | Start | End | Notes |
| --- | --- | --- | --- |
| 1 | 2026-10-25 | | exploration, repo setup, working process |

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

## D1. Executor and drivers talk over core NATS request/reply; the executor asks for the truth

- **Question:** how commands reach the drivers and outcomes get back, so the record
  stays true when messages are lost, duplicated or late, and processes or networks drop.
- **Chosen (option A):**
  - `lab.<device>.run {job_id, run_id, step}`: the driver replies `accepted`, `known` or
    `busy` straight away, and runs the job in the background.
  - `lab.<device>.job {job_id}`: the driver answers from the instrument ledger and its own
    upload state.
  - An executor loop (~500 ms) asks about every in-flight job, applies the answers with
    guarded updates, and dispatches what `NextSteps` allows.
  - Later, optionally: a lossy core "job changed" nudge for latency.
- **Correctness comes from these, not the transport:**
  1. The executor writes `job_id` and `attempt` to Postgres before sending.
  2. Predictable `job_id = <run>/<step>/<attempt>`. In one handler, the driver checks its
     in-memory claims and the ledger for this `job_id`, then claims it. A duplicate is
     answered with the job's state and never runs again.
  3. Guarded state changes in Postgres. A late or duplicate answer cannot move a step
     backwards or touch another attempt.
  4. **Attempt rule:** never start attempt n+1 while attempt n's outcome is unknown. For
     a hang, wait until the ledger says `hung`. The liquid handler never retries.
  5. Plate reader data goes under a predictable key (`runs/<run>/<step>/<job_id>.csv`). A
     read is `completed` only once that object is stored and its sha256 equals the
     ledger's.
- **Alternatives:**
  - *B, JetStream both ways* (work-queue command stream per device, event stream to the
    executor). It buffers across outages, but adds AckWait/heartbeat/dedupe-window tuning,
    commands still queued for cancelled runs, and a second place where state lives. It
    still needs everything above (duplicates, the DB-write/publish gap, hangs). It buys
    buffering, not correctness.
  - *C, hybrid* (request/reply commands, JetStream events, plus a status query). Two
    mechanisms and two recovery paths, and it still needs A's query.
- **Why A:** every guarantee rests on things that are durable on their own (Postgres, the
  ledger, storage), not on a message getting through. Recovery is the normal path, so
  every run exercises it. It is the smallest system to explain.
- **Trade-offs accepted:**
  - Up to one tick of latency. The record's times come from the ledger, so it doesn't
    affect accuracy.
  - The executor does the per-device queueing across runs.
  - No JetStream, although the stack switches it on.
  - **Deferred:** plate reader bytes live only in driver memory until uploaded, so a driver
    restart in that window loses them. The record then shows a failed attempt ("data
    lost"), and a retry. A local data file on the `/ledger` volume would close this. It is
    listed for NOTES §5.
- **Rests on (measured with throwaway programs in `tmp/`, 2026-10-26):**
  - The instrument does **not** dedupe by JobID: the same job run again after it
    completed gave `[completed completed]`. So the driver's check is required.
  - Concurrent `Run` with the same job: one runs, the other gets `ErrBusy`.
  - A hung job keeps the device busy, with the ledger saying `running`, after the caller
    gives up. It turns `hung` after `HANG_DURATION`.
  - A restart mid-job leaves `hung`, "instrument restarted during the job".
  - Plate reader `Outcome.Data` sha256 equals the ledger's `data_sha256`.
  - Core NATS callbacks on one subscription run one at a time (5 messages × 200 ms took
    1.0 s, max 1 concurrent). So check-then-claim in the handler can't race.
  - *Inferred:* the ledger is written before the work starts and survives restarts
    (`instrument.go:166`, `:311`).
- **Would revisit if:**
  - a disconnect makes requests hang for minutes (not yet measured);
  - several runs need to queue per device;
  - the event history should be part of the record.
- **Slice / PR:** first slice is the driver protocol.
