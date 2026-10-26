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
