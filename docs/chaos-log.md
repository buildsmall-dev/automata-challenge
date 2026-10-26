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
