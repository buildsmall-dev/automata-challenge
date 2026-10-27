# Chaos log

One entry per experiment, newest last. Feeds NOTES.md §4–5.

## Template

```
## C<n>. <fault> during <moment>

- **Setup:** commit, env vars (CHAOS_SEED, *_FAIL_PCT, *_HANG_PCT), workflow
- **Command:** exactly what was run, so it can be repeated
- **Predicted:** what we expected the record and the ledgers to say
- **Observed:** what they said (run id, key output, trimmed)
- **Record true?** yes / no / stuck, and which step disagreed
- **Understanding:** what the gap between prediction and result taught us
- **Follow-up:** slice / PR that fixed it, or "accepted" with a reason
```

---

## C1. Network partition of a NATS client (before any application code)

- **Setup:** throwaway probe `tmp/_natsprobe` (local, not committed). Probe container on
  `lab-network`, peer on the host never partitioned. Every 200ms a core publish, every 1s a
  request (2s timeout), every 1s a JetStream publish (5s timeout, Msg-Id). nats.go v1.53.1,
  nats-server 2.10.29, Docker Desktop for Mac.
- **Command:** `tmp/_natsprobe/run.sh <label> <ping 0|2s> <secs>`, which does
  `docker network disconnect`/`connect --ip` like `chaos.sh disconnect`. Runs: default pings
  6s and 20s; 2s pings 6s and 20s.
- **Predicted:** with default pings the client won't notice and core messages in the gap are
  lost; requests fail cleanly.
- **Observed:**

  | Run | Client noticed | Core lost | Timed-out requests later delivered | JS "failed" → stored |
  | --- | --- | --- | --- | --- |
  | default, 6s | never | 0 (late) | not logged | 1 → 1 |
  | default, 20s | never | 0 (late) | 6 of 6, ~20s late | 3 → 3 |
  | 2s ping, 6s | stale after ~6s | 0 | 2 of 2 | 1 → 1 |
  | 2s ping, 20s | stale after ~6s | 30 | 4 of 6 late, 2 lost | 3 → 2 |

- **Record true?** n/a (no application yet).
- **Understanding:** prediction was wrong twice. With default pings the TCP connection
  survives, so nothing is lost but everything is late, including requests the sender already
  gave up on. And 7 of 8 JetStream publishes that reported a timeout were stored. A timeout
  is "unknown", never "didn't happen". Short pings trade lateness for loss.
- **Follow-up:** D1 (claim before run, same-job_id resend, Msg-Id retries, idempotent apply,
  explicit PingInterval).

## C2. Storage down and the plate reader restarted between a read and its upload

- **Setup:** branch `slice/driver-replay` (R1 + D3), default stack, no instrument faults,
  default workflow (PCR Amplification).
- **Command:** start a run; when `read_plate` is `running` on the plate reader's ledger,
  `docker compose stop storage`; when it is `completed` there,
  `docker compose restart plate-reader-1`; then `docker compose start storage`. Finally,
  stand in for the dead process's delayed upload: rebuild the read's bytes (the instrument's
  `measure` depends only on the job id) and `curl -X PUT` them to `runs/<job>.csv`.
- **Predicted:** the driver retries the upload while storage is down; the restart loses the
  bytes; the restarted driver replays `finished`, waits for storage, finds no object after
  the grace and reports `data_lost`. Under D3 the read is unresolved and the run stays
  `running`. When the late upload lands, the executor's recheck finds it and the read and
  run complete.
- **Observed:** `run-efacd6fdca88`. Driver log: `store data of ...: dial tcp: lookup storage
  ... no such host; retrying`, then after the restart `data of run-efacd6fdca88/read_plate/1
  is not in storage and this process never had it; reporting it lost`. API: run `running`,
  read `running`, error "driver lost the data before it was confirmed stored; an earlier
  upload may still appear in storage". Rebuilt bytes' sha256 `652dd17c...` = the ledger's.
  After the PUT, within one recheck (10s): run `completed`, `finished_at` 13:18:53 (ledger
  time), read `completed`, `data_ref` `s3://lab-data/runs/run-efacd6fdca88/read_plate/1.csv`.
  `acceptance.sh` afterwards: 7/7.
- **Record true?** yes, at every point. Before D3 the same sequence finished the run as
  `failed` ("data was lost") and the late upload then contradicted it (seen in R1's first
  version on `run-8c366ff2ecb6`, which is still `failed`: K41).
- **Understanding:** "absent now" is not "lost". Without conditional writes in storage
  (measured: SeaweedFS 3.62 ignores `If-None-Match`) nothing can make loss final, so the
  record can only hold the question open and keep looking.
- **Follow-up:** D3, R1 (PR #11). Ending such a read is a decision: retry policy or
  operator "declare lost" (K39, K6).

## C3. Hang faults on every device under D4 (retry policy)

- **Setup:** `dev` at `91f31c9` (D4 complete: attempts, store lookup, policy). Executor policy
  defaults: `REPEAT_SAFE=plate-reader`, `REREAD_ON_DATA_LOSS=plate-reader`, `MAX_ATTEMPTS=3`.
  `LH_HANG_PCT=INC_HANG_PCT=PR_HANG_PCT=30`, all `*_FAIL_PCT=0`, `HANG_DURATION=30s`.
  - **C3a:** `CHAOS_SEED=0`, one PCR Amplification run, then one Triple Assay run.
  - **C3b:** `CHAOS_SEED=17`, one Triple Assay run. This seed was chosen from the roll table so
    that only `plate_read` hangs on its first attempt, to exercise the one retry D4 allows
    for a hang.
- **Command:** `CHAOS_SEED=<seed> LH_HANG_PCT=30 INC_HANG_PCT=30 PR_HANG_PCT=30 docker compose up -d
  liquid-handler-1 incubator-1 plate-reader-1`, then `POST /runs {"workflow_name": ...}` and
  `POST /runs/<id>/start`. Wait until the run is terminal or 150s pass.
- **Roll table** (computed with `instrument.go`'s `roll`; the instrument's attempt counter is our
  attempt − 1; HANG if roll < 30):

  | seed | step | device | attempt 1 / 2 / 3 |
  |---|---|---|---|
  | 0 | fill_sample_plate | liquid-handler-1 | 13 HANG / 19 HANG / 52 |
  | 0 | incubate_samples, fill_buffer_plate, warm_reagent_plate, combine, read_plate | — | first attempt ok |
  | 0 | fill_reagent_plate | liquid-handler-1 | 22 HANG / 65 / 20 HANG |
  | 0 | prepare, pipette, plate_read, report | — | first attempt ok |
  | 0 | incubate | incubator-1 | 26 HANG / 7 HANG / 93 |
  | 17 | prepare, pipette, incubate, report | — | first attempt ok |
  | 17 | plate_read | plate-reader-1 | 26 HANG / 83 / 91 |

- **Predicted** (written before running):
  - **C3a, PCR:**
    - The liquid handler's ledger has exactly one execution for the run: `fill_sample_plate/1`,
      `hung`, about 30s long.
    - The record has `fill_sample_plate` `failed`, "instrument hung; outcome unknown; not
      repeated: liquid-handler-1 is not safe to repeat", with the ledger's times. During the
      hang it is `running` with no error.
    - The other six steps are `pending` and never commanded; no other ledger has an execution
      for this run.
    - The run is `failed`, with `finished_at` the hung execution's ledger `finished_at`.
    - Path: driver `waitLedger` → `finished(hung)` → lifecycle `ended` → `Policy.next` gives up →
      `finish`.
  - **C3a, Triple Assay:**
    - `prepare`, `pipette` and `plate_read` are `completed`; `plate_read`'s object exists and its
      sha256 equals the ledger's.
    - `incubate` is `failed`, "…; not repeated: incubator-1 is not safe to repeat"; its ledger
      has `incubate/1` `hung`.
    - `report` is `pending` and never commanded. The run is `failed`. No `/2` job on any device.
  - **C3b, Triple Assay:**
    - The plate reader's ledger has `plate_read/1` `hung`, then `plate_read/2` `completed`
      (started about 30s later, once the hang is reported), then `report/1` `completed`.
    - The record has `plate_read` `completed` with `data_ref` `runs/<run>/plate_read/2.csv`,
      the object's sha256 equal to attempt 2's `data_sha256`, and attempt 2's ledger times.
    - The run is `completed`.
    - `acceptance.sh`'s check 5 would call this a step that ran twice (K52); it isn't run here.
- **Observed** (measured; ledger times in seconds past 01:2x UTC, 2026-10-27):
  - **C3a PCR, `run-e3abbca2ff3a`:**
    - Ledger: `fill_sample_plate/1` `hung`, from 21:48.5345 to 22:18.5367 (30.0s). Nothing
      else on any device.
    - Record: `fill_sample_plate` `failed`, "instrument hung; outcome unknown; not repeated:
      liquid-handler-1 is not safe to repeat", with the same two times. It was `running`
      with no error at every 5s poll during the hang. Six steps `pending`.
    - Run `failed`, `finished_at` 22:18.536736, the ledger's.
  - **C3a Triple Assay, `run-899fe8ab6674`:**
    - Ledger: `prepare/1`, `pipette/1`, `plate_read/1` `completed`; `incubate/1` `hung`
      (22:38.4810 to 23:08.4867).
    - Record: the three `completed`; `incubate` `failed`, "…; not repeated: incubator-1 is not
      safe to repeat"; `report` `pending`; run `failed`.
    - `plate_read/1.csv` sha256 `4c7294…c8f9` = ledger `data_sha256`.
  - **C3b Triple Assay, `run-21f4165a5d5c`:**
    - Ledger (plate reader): `plate_read/1` `hung` from 23:37.4677 to 24:07.4690; then
      `plate_read/2` `completed`, starting at 24:07.5000 (31ms after the hang was recorded)
      and ending at 24:09.5020; then `report/1` `completed`.
    - Record: every step `completed`, run `completed`. `plate_read` has `data_ref`
      `…/plate_read/2.csv` and attempt 2's times; the lifecycle keeps both attempts, with
      `Decided: 2`.
    - Storage: `plate_read/2.csv` sha256 `276b13…bcb5` = attempt 2's ledger sha; there is no
      `plate_read/1.csv` (404); `report/1.csv` `c65fa9…3a9e` = its ledger sha.
    - Executor logs: no errors and no unknown jobs.
- **Record true?** Yes, in all three runs:
  - Every status, outcome and time agrees with a ledger entry.
  - Every `completed` read is backed by an object whose hash matches its ledger.
  - No step claimed success over a `hung` entry, and nothing was stuck.
- **Understanding:**
  - **Every prediction held.** The fault roll is deterministic and has no run id, so the
    policy's outcome for a seed can be read off the table before running. At seed 0, every
    PCR run with `LH_HANG_PCT` above 13 fails at its first step, by design: the liquid handler
    isn't safe to repeat.
  - **Missed in the prediction:** `report` is also a plate-reader step, so it stores data too.
    It was handled correctly.
  - **A retry costs a full `HANG_DURATION`.** It waits for the instrument's own `hung` (no
    timer, D4), then goes at once (31ms measured). C3b took 36s instead of about 6s.
  - **New gap:** with retries, the REST step and `/timeline` show only the attempt that decided
    the step. `plate_read`'s `started_at` is 24:07.5, and the timeline puts it at offsets
    32.1s–34.1s. Every time shown is a true ledger time. But the plate reader was also
    occupied by `plate_read/1` from offset 2.0s to 32.0s, and neither shows that. The record
    isn't false, but it's incomplete about when the device was busy (K53).
- **Follow-up:** no fix slice; the record was true. K53 goes to GraphQL (an attempts history
  per step) and possibly the timeline. K52 (acceptance check 5 against retried steps) still
  stands; acceptance wasn't run with faults on.

## C4. Final pass: process crashes, partitions, a storage outage and random faults

- **Setup:**
  - `dev` at `4d27bd4` (PR20: GraphQL). The executor policy is at its defaults:
    `REPEAT_SAFE=plate-reader`, `REREAD_ON_DATA_LOSS=plate-reader`, `MAX_ATTEMPTS=3`.
  - Every `*_FAIL_PCT` and `*_HANG_PCT` is 0 except in C4j. `HANG_DURATION=30s` and
    `CHAOS_SEED=0` unless stated.
  - The workflow is PCR Amplification. Each step takes about 2s; `read_plate` runs from about
    8.2s to 10.2s after start.
  - Each run is checked with `scripts/check-run.py`. It compares every step and attempt in the
    record (REST and GraphQL) with all three ledgers, every ledger execution with an attempt,
    and every `data_ref` object's sha256 with the ledger's `data_sha256`.
- **Command:** `scripts/experiments/c4-fault.sh <case>`. It creates and
  starts a run, injects the fault at the offset given, waits until the run is terminal (or
  120s), then runs `check-run.py`. C4i and C4j use `FOR=<secs> scripts/experiments/c4-random.sh
  <seed> <out>`, which runs `./scripts/chaos.sh random` while starting runs back to back.
- **Predicted** (written before running):
  - **C4a, executor killed at about 3s** (incubate and fill_reagent running):
    - The instruments finish both steps while the executor is down. The `started` and
      `finished` events wait in `LAB_EVENTS`, and the restarted executor applies them.
    - Run `completed`; 7 ledger executions, each once; no PROBLEM. Only delay.
  - **C4b, executor killed at about 9s** (read_plate running):
    - The read completes and uploads while the executor is down; `stored` waits in the
      stream. On restart the executor reads the object back.
    - `read_plate` `completed` with a matching hash; run `completed`.
  - **C4c, plate-reader-1 killed at about 9s, mid-read, started 3s later:**
    - Ledger: `read_plate/1` `hung`, "instrument restarted during the job", with
      `finished_at` at the restart.
    - `replay` reports it. The plate reader is repeat-safe, so `read_plate/2` runs and
      completes.
    - Record: `read_plate` `completed`, with 2 attempts (1 `HUNG`, 2 decided) and `data_ref`
      `…/read_plate/2.csv`. Run `completed`; 8 ledger executions; no PROBLEM.
  - **C4d, liquid-handler-1 killed at about 7s, mid-combine, started 3s later:**
    - Ledger: `combine/1` `hung`.
    - Record: `combine` `failed`, "instrument hung; outcome unknown; not repeated:
      liquid-handler-1 is not safe to repeat".
    - `read_plate` `pending` and never commanded. Run `failed`; 6 ledger executions.
  - **C4e, incubator-1 killed at about 1s, while idle, started 5s later:**
    - The command for `incubate_samples` (about 2s) finds no responder. The executor sends it
      again until the driver is back.
    - Run `completed`, about 4s late; each step once.
  - **C4f, executor cut off the network for 8s at about 3s** (NATS and Postgres both):
    - The instruments finish the two running steps; events wait.
    - After reconnecting, the run continues. Run `completed`; no PROBLEM.
  - **C4g, plate-reader-1 cut off for 10s at about 8.5s** (the read ends while it's cut off):
    - The instrument carries on. The driver retries the upload and the publish until
      reconnected; the step is `running`/`storing` meanwhile.
    - Run `completed`; one `read_plate` execution; hash matches.
  - **C4h, storage down for 15s at about 7s** (the read ends while storage is down):
    - The driver's upload retries; `read_plate` stays `running` (phase `STORING`) with no
      `data_ref` until storage is back.
    - Run `completed` about 8s late; hash matches.
  - **C4i, `chaos.sh random --seed 7 --for 60`, and again with `--seed 11`,** with runs back to
    back:
    - Every run `completed`, each step once, no PROBLEM. These faults only delay: nothing
      physical fails.
  - **C4j, as C4i with `--seed 11`, plus instrument faults** (`INC_FAIL_PCT=20 PR_HANG_PCT=30
    HANG_DURATION=5s CHAOS_SEED=3`):
    - Runs end `completed` or `failed`. A failed step names its cause; a hung read is
      repeated up to 3 attempts.
    - No PROBLEM, and no run stuck `running` once the faults stop.
  - **C4j, added after the first seed-3 attempt** (the prediction above didn't compute the
    roll, so it couldn't say which steps would fault). From `instrument.go`'s `roll`, where the
    instrument's attempt is ours − 1:

    | seed | incubate_samples (fail <20) | warm_reagent_plate (fail <20) | read_plate (hang <30) |
    |---|---|---|---|
    | 3 | ok / ok / ok | ok / ok / ok | H / H / H |
    | 13 | F / ok / ok | ok / ok / F | H / ok / H |
    | 29 | ok / ok / ok | F / F / F | H / H / H |

    - **Seed 13, with `chaos.sh random --seed 7 --for 90`:** every run `completed`, 9 ledger
      executions. `incubate_samples` has 2 attempts (1 `FAILED`, retryable; 2 decided).
      `read_plate` has 2 attempts (1 `HUNG` about 5s; 2 decided, `…/read_plate/2.csv`).
    - **Seed 29, with `--seed 11 --for 90`:** every run `failed`.
      - `warm_reagent_plate` `failed`, "instrument error; not repeated: 3 attempts made".
      - `combine` and `read_plate` `pending`, never commanded.
      - 7 ledger executions.
- **Observed** (measured, 2026-10-27, 09:18–09:33 UTC; times are offsets from the run's start):
  - **C4a `run-57484bd1dae9`, C4b `run-16b1ccfb56d8`:** `completed`, 7 executions each.
    - `chaos.sh kill` starts the executor again at once, so it was down for under a second, and
      the runs were barely slowed (11s and 10s).
  - **C4c `run-f03f5e73fc41`:**
    - Ledger: `read_plate/1` `hung`, "instrument restarted during the job", 09:19:37.014 to
      09:19:41.799 (the restart). Then `read_plate/2` `completed`, starting 44ms later.
    - Record: 2 attempts (1 `HUNG`/`UNRESOLVED`, 2 decided), `data_ref …/read_plate/2.csv`
      with a matching hash. Run `completed`, 8 executions.
    - REST `/timeline` shows only attempt 2 (13.0–15.0s); GraphQL shows both (K53).
  - **C4d `run-3f3c4d1482bf`:**
    - Ledger: `combine/1` `hung`, ending at the driver's restart.
    - Record: `combine` `failed`, "instrument hung; outcome unknown; not repeated:
      liquid-handler-1 is not safe to repeat", from 6.2s to 11.0s. `read_plate` `pending`.
    - Run `failed`, 6 executions.
  - **C4e `run-f3625f8d5871`:**
    - The executor logged "no responders available … asking again" once a second for 6s.
    - `incubate_samples` ran at 7.7s, not 2s. Run `completed`, 7 executions, 16s.
  - **C4f `run-9dae9e4fbd1b`, repeated as `run-560ca12f96ee`:** `completed`, 7 executions, but
    41s instead of the predicted about 11s.
    - `fill_buffer_plate` started at 34.1s: the two steps before it had ended at 4.1s, and
      the partition ended at 11s.
    - Repeated while polling the consumer (NATS `/jsz`): `num_ack_pending` was 2 from just
      after the cut until 30s later, then 0, and the run moved on at once.
    - So the `finished` events were delivered to the executor's pull request as it was cut
      off, and redelivered only after `ackWait` (30s).
    - During the gap the record said `running` for steps that had ended on the instrument:
      behind, not false.
  - **C4g `run-e23df920dd80`:** `completed`, 7 executions, 21s.
    - The read ended while the plate reader was cut off; its upload and events went through
      after reconnecting.
  - **C4h `run-937c024e59ae`:** `completed`, 40s, hash matches. Storage was down 15s, but the
    read completed 17s after `storage up`, not about 5s.
    - The SeaweedFS log shows "wait to connect to filer" until about 09:22:10. The driver's
      upload, retried every 0.5s, succeeded at 09:22:11.
    - The delay is storage's own restart, not ours. Even a 2s outage costs about 16s.
  - **C4i** (seed 7: `run-3698263604dd`, `run-e337f59e87cc`; seed 11: `run-d1f3b9c7eb52`,
    `run-77a29ce9c490`):
    - Faults: executor kill, executor partitions of 7s, 4s and 4s, storage down 7s and 2s,
      and partitions of each driver.
    - Every run `completed`, 7 executions. Only two runs fit in each 60s window, because of
      the ackWait and storage restart delays above.
  - **C4j, seed 3** (`run-4ea7b3d1579a`, `run-c3753f954e48`, `run-0df33219a2a0`,
    `run-0685eebafb4e`):
    - Every run `failed`: `read_plate` hung on all three attempts ("…; not repeated: 3
      attempts made"), 9 executions.
    - True, but the same for every run, which is what the roll table shows.
  - **C4j, seed 13** (`run-ab45fe5f12de`, `run-a7eceae3b8fe`): as predicted.
    - `completed`, 9 executions.
    - `incubate_samples` and `read_plate` each have 2 attempts. The second decided, and the
      read has `…/read_plate/2.csv` with a matching hash.
  - **C4j, seed 29** (7 runs, `run-0a9a50f63735` … `run-a9685beebe97`): as predicted.
    - Every run `failed`: `warm_reagent_plate` "instrument error during warm_reagent_plate;
      not repeated: 3 attempts made".
    - `combine` and `read_plate` `pending` and absent from every ledger; 7 executions.
  - **All 26 runs re-checked at the end** with `check-run.py`, after the faults stopped and
    storage was back: 26 ok, 0 PROBLEM.
    - The checker was itself tested against doctored ledgers. It flagged all six: outcome
      differs, time differs, sha differs, a job twice, an unrecorded execution, and a
      completion with no execution.
    - It also fails closed: while storage was still starting, it called a good read a
      PROBLEM because it couldn't read the object.
  - **Afterwards:** every `*_PCT` back to 0, `CHAOS_SEED=0`, `HANG_DURATION=30s`.
    `acceptance.sh`: 7/7 on PCR Amplification (`run-f23d73be1195`) and 7/7 on Triple Assay
    (`run-76615f7dbc9e`).
- **Record true?** Yes, in all 26 runs, at every check:
  - every outcome and time matched a ledger entry;
  - every ledger execution is an attempt in the record, and no job ran twice;
  - every completed read's object has the ledger's hash;
  - no step was completed over a `hung` entry, and none stuck.
- **Understanding:**
  - **Wrong about time, not truth.** Two predictions missed on how long recovery takes:
    - *An executor partition costs up to `ackWait` (30s), not its own length.* Events handed
      to a pull request that never reaches the client wait for redelivery (K62).
    - *A storage outage costs at least SeaweedFS's restart* (about 16s here), whatever its
      length. This is outside our code.
  - **The first C4j prediction was vague,** because it didn't compute the roll. The faults come
    from seed, device, step and attempt, never the run (as C3 found), so one seed gives every
    run the same faults. Choosing seeds from a roll table is what made seeds 13 and 29 test
    retries and a failure.
  - **A driver crash mid-job behaves like a hang** (`instrument.go:341`):
    - The ledger closes it as `hung` at the restart, and `replay` reports it.
    - The plate reader retries it; the liquid handler doesn't.
    - The record's times for a crashed attempt end at the restart, not when the work
      stopped. That is the ledger's time, the best evidence there is.
- **Follow-up:**
  - No fix slice: no record was false or stuck.
  - K62 (ackWait latency after an executor partition) goes to `docs/concerns.md`.
  - `scripts/check-run.py` and `scripts/experiments/c4-*.sh` are committed so this can be
    rerun.
