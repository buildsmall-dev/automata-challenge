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
