# Chaos experiments

Four experiments that broke the system on purpose, to check that the run record stays true.
Each one followed the same method:

1. **Predict first.** Before running, write down what the record and the instrument ledgers
   should say, and which code handles the fault.
2. **Run it** from a known state, injecting the fault at a chosen moment.
3. **Compare** the record with the evidence: each step's status, times and attempts against
   the ledgers, and each read's stored object against the ledger's sha256.
4. **Explain any wrong prediction.** A wrong prediction means we misunderstood something,
   even when the record turned out true.

This page gives each experiment's results and lessons. The working log in
[`../forai/chaos-log.md`](../forai/chaos-log.md) has the full detail: every run id,
timestamp and log line. For the terms, see
[decisions.md](decisions.md#terms-used-on-this-page).

| | Fault | Runs | Record true? | Main lesson |
| --- | --- | --- | --- | --- |
| [C1](#c1-a-nats-client-cut-off-from-the-network) | A NATS client cut off from the network | (before any code) | n/a | A timeout means "unknown", not "didn't happen". |
| [C2](#c2-storage-down-and-the-plate-reader-restarted-mid-upload) | Storage down, plate reader restarted mid-upload | 1 | yes | "Not in storage now" is not "lost". |
| [C3](#c3-hangs-on-every-device) | Hangs on every device | 3 | yes | Outcomes under a seed can be computed in advance. |
| [C4](#c4-crashes-partitions-storage-outages-and-random-faults) | Crashes, partitions, storage outages, random faults | 26 | yes | Recovery is slower than predicted, but never false. |

## How to check a run

`scripts/check-run.py <run-id>...` compares a run's record with all three ledgers and with
storage. It reports a PROBLEM if:

- a job in a ledger is missing from the record, or a job ran twice;
- an attempt's outcome or times differ from its ledger entry;
- a completed step has no completed ledger entry, or a read's stored object doesn't have
  the ledger's sha256;
- a pending, failed or cancelled step contradicts a ledger;
- a finished run still has a step in flight.

The checker was itself tested against six kinds of doctored ledger, and caught all six. It
fails closed: if it can't read an object, it reports a PROBLEM.

---

## C1. A NATS client cut off from the network

**Why:** to learn how NATS really behaves on a network partition before designing the
messaging (D1).

**What we did:** a throwaway probe in a container on the lab network, with a peer on the
host that was never cut off. Every 200ms it sent a plain publish, every second a request
(2s timeout), and every second a JetStream publish (5s timeout, with a message id). We cut
the probe off the network for 6s and for 20s, with the default keepalive pings and with
2s pings. (nats.go 1.53.1, nats-server 2.10.29, Docker Desktop for Mac.)

**Expected:** with default pings, the client wouldn't notice the cut, and messages sent
during it would be lost. Requests would fail cleanly.

**What happened:**

| Cut | Client noticed | Plain publishes lost | Timed-out requests delivered later | JetStream publishes that "failed" but were stored |
| --- | --- | --- | --- | --- |
| default pings, 6s | never | 0 (all late) | not logged | 1 of 1 |
| default pings, 20s | never | 0 (all late) | 6 of 6, about 20s late | 3 of 3 |
| 2s pings, 6s | after about 6s | 0 | 2 of 2 | 1 of 1 |
| 2s pings, 20s | after about 6s | 30 | 4 of 6 | 2 of 3 |

**What we learned:** the prediction was wrong twice.

- With default pings the TCP connection survives the cut, so nothing is lost, but
  everything is late, **including requests the sender had already given up on**.
- **7 of 8 JetStream publishes that reported a timeout had been stored.**

So a timeout means "unknown", never "didn't happen". Shorter pings trade lateness for loss.
This shaped D1: a driver claims a job before starting it, a resend reuses the same job id,
event publishes are retried with a message id, applying an event twice is harmless, and
the ping interval is set explicitly (5s).

**Reproducing it:** the probe was a local script and isn't in the repository, so C1 can't
be fully reproduced from it (K8).

---

## C2. Storage down, and the plate reader restarted mid-upload

**Why:** to test D3. A driver that restarts before uploading a read loses its bytes, but
the original upload might still land later.

**What we did** (default stack, no instrument faults, PCR Amplification):

1. Start a run. While `read_plate` is running, stop storage.
2. Once the read has completed on the instrument, restart the plate reader's driver. The
   bytes, held only in its memory, are gone.
3. Start storage again.
4. Stand in for the dead process's late upload: rebuild the read's bytes (the simulated
   instrument's data depends only on the job id) and `PUT` them to `runs/<job>.csv`.

**Expected:** the driver retries its upload while storage is down. After the restart, the
new driver finds no object and reports `data_lost`. The read is unresolved and the run
stays `running`. When the late upload lands, the executor's 10s recheck finds it, and the
read and the run complete.

**What happened:** as predicted, at every point.

- The driver logged that storage was unreachable and kept retrying. After the restart it
  reported the data lost.
- The API showed the run and the read `running`, with the error "driver lost the data
  before it was confirmed stored; an earlier upload may still appear in storage".
- After the `PUT`, within one recheck, the read was `completed` with the right `data_ref`
  and hash, and the run `completed` with the ledger's end time.
- The acceptance script passed afterwards (7/7).

An earlier version, which failed the read after a grace period, finished the same run as
`failed`, and the late upload then contradicted it. That is why D3 exists.

**What we learned:** "absent now" is not "lost". SeaweedFS 3.62 ignores conditional writes
(measured), so nothing can make a loss final. The record can only keep the question open
and keep looking.

**Since then:** C2 ran before the retry policy (D4). Today the `data_lost` would also start
a second read at once, and the step would complete from whichever attempt is verified
first. Unit tests cover this (`TestLostReadIsReadAgain`,
`TestLostReadIsFoundWhileItsReReadIsStuck`); it wasn't re-run live.

---

## C3. Hangs on every device

**Why:** to test the retry policy (D4) under hangs, with the default settings: only the
plate reader is safe to repeat, at most 3 attempts.

**What we did:** set a 30% hang rate on every device (`LH_HANG_PCT`, `INC_HANG_PCT`,
`PR_HANG_PCT` = 30), `HANG_DURATION=30s`, then:

- **C3a:** seed 0, one PCR Amplification run, then one Triple Assay run;
- **C3b:** seed 17, one Triple Assay run.

```bash
CHAOS_SEED=<seed> LH_HANG_PCT=30 INC_HANG_PCT=30 PR_HANG_PCT=30 \
  docker compose up -d liquid-handler-1 incubator-1 plate-reader-1
curl -s -XPOST localhost:5002/runs -d '{"workflow_name": "..."}'   # then POST /runs/<id>/start
```

**Predicting from the roll.** The instrument decides each fault from
`sha256(seed:device:step:attempt) % 100`, with no run id. So under one seed, each step's
fate on each attempt is fixed, and can be computed before running. (The instrument counts
attempts from 0, so its attempt is ours − 1.) A step hangs if its roll is below 30:

| Seed | Step | Device | Attempt 1 / 2 / 3 |
| --- | --- | --- | --- |
| 0 | `fill_sample_plate` | liquid handler | 13 hang / 19 hang / 52 |
| 0 | `fill_reagent_plate` | liquid handler | 22 hang / 65 / 20 hang |
| 0 | `incubate` | incubator | 26 hang / 7 hang / 93 |
| 0 | every other step | | first attempt ok |
| 17 | `plate_read` | plate reader | 26 hang / 83 / 91 |
| 17 | every other step | | first attempt ok |

Seed 17 was chosen from this table because only `plate_read` hangs, which tests the one
case where D4 retries a hang.

**Expected and what happened:** every prediction held.

| Run | Expected | Observed |
| --- | --- | --- |
| C3a, PCR | `fill_sample_plate` hangs for 30s and isn't repeated (the liquid handler isn't safe to repeat). The step is `failed`, the other six steps are never sent, and the run is `failed`. | Exactly that. During the hang the step showed `running` with no error. The step's and the run's times are the ledger's. |
| C3a, Triple Assay | `incubate` hangs and isn't repeated. `prepare`, `pipette` and `plate_read` complete; `report` is never sent; the run is `failed`. | Exactly that. `plate_read`'s object matches its ledger hash. |
| C3b, Triple Assay | `plate_read` attempt 1 hangs; attempt 2 runs once the hang is reported, and completes. The run is `completed`. | Exactly that. Attempt 2 started 31ms after the hang was recorded. `data_ref` is `…/plate_read/2.csv`, matching attempt 2's hash. The record keeps both attempts. |

**What we learned:**

- **Outcomes under a seed can be computed in advance.** At seed 0, every PCR run with a
  liquid-handler hang rate above 13% fails at its first step, by design.
- **A retried hang costs the full `HANG_DURATION`.** The retry waits for the instrument's
  own report of the hang (no timer, D4), then goes at once. C3b took 36s instead of about 6s.
- **Missed in the prediction:** `report` is also a plate-reader step, so it stores data
  too. The code handled it correctly.
- **A gap (K53):** REST and `/timeline` showed only the attempt that decided the step. The
  plate reader was also busy with attempt 1 for 30s, and nothing showed it. Every time
  shown was true, but the picture of when the device was busy was incomplete. GraphQL now
  lists every attempt (PR #20).

**Follow-up:** no fix needed; the record was true.

---

## C4. Crashes, partitions, storage outages and random faults

**Why:** a final pass over everything that can fail, once the whole system was built
(through GraphQL, PR #20), with the default retry settings.

**Setup:** PCR Amplification, about 2s per step; `read_plate` runs from about 8.2s to 10.2s
after the start. No instrument faults except in C4j. Every run was checked with
`check-run.py`.

```bash
scripts/experiments/c4-fault.sh <case>                    # C4a–C4h: one run, one fault
FOR=<secs> scripts/experiments/c4-random.sh <seed> <out>   # C4i–C4j: chaos.sh random, runs back to back
```

**Expected and what happened** (times are from the run's start):

| Case | Fault | Expected | Observed |
| --- | --- | --- | --- |
| C4a | executor killed at 3s | events wait in the stream; run `completed` | as expected. The executor was down under a second. |
| C4b | executor killed at 9s, mid-read | the read uploads; `stored` waits; the executor verifies it on restart | as expected |
| C4c | plate-reader driver killed at 9s, mid-read, back 3s later | ledger says `hung` for attempt 1; it's retried, and attempt 2 completes | as expected. 8 jobs in the ledger. REST's timeline shows only attempt 2; GraphQL shows both. |
| C4d | liquid-handler driver killed at 7s, mid-`combine` | `combine` `hung`, not repeated; `read_plate` never sent; run `failed` | as expected |
| C4e | incubator driver killed at 1s, idle, back 5s later | its command finds no one and is sent again until the driver is back; run `completed` about 4s late | as expected. The step started at 7.7s instead of 2s. |
| C4f | executor cut off for 8s at 3s | run continues after the cut; `completed` about 11s total | **took 41s.** See below. |
| C4g | plate-reader driver cut off for 10s at 8.5s | the read ends during the cut; upload and events go through after it | as expected |
| C4h | storage down for 15s at 7s | the read waits in `storing` with no `data_ref`; `completed` about 8s late | **took 40s.** See below. |
| C4i | `chaos.sh random`, seeds 7 and 11, 60s each | these faults only delay: every run `completed` | as expected (4 runs) |
| C4j | as C4i, plus instrument faults: 20% incubator failures, 30% plate-reader hangs, 5s hangs | runs end `completed` or `failed`, each with its cause; none stuck | as expected (13 runs, seeds 3, 13 and 29) |

**C4f: an executor partition costs 30s, whatever its length (K62).** The two steps
running at the cut ended at 4.1s, the cut ended at 11s, but the next step started at
34.1s. Repeating the case while watching the consumer showed why: two events were
"delivered, not acked" from just after the cut until 30s later. JetStream had handed them
to the executor's pending pull request just as it was cut off, so they were lost in
transit and redelivered only after the 30s ack timeout. Meanwhile the record said
`running` for steps that had ended: behind, not false. Lowering the ack timeout would
reduce the cost.

**C4h: a storage outage costs at least SeaweedFS's restart.** Storage was down 15s, but
the read completed 17s after storage was started again. SeaweedFS logged "wait to connect
to filer" until then; the driver's upload, retried every 0.5s, succeeded a second later.
Even a 2s outage costs about 16s. This is outside our code.

**C4j: choosing seeds from the roll.** The first C4j prediction was vague because it
didn't compute the roll. Computed afterwards (fail below 20, hang below 30):

| Seed | `incubate_samples` | `warm_reagent_plate` | `read_plate` | Result |
| --- | --- | --- | --- | --- |
| 3 | ok / ok / ok | ok / ok / ok | hang / hang / hang | every run `failed`: `read_plate` hung 3 times |
| 13 | fail / ok / ok | ok / ok / fail | hang / ok / hang | every run `completed`: two steps retried once |
| 29 | ok / ok / ok | fail / fail / fail | hang / hang / hang | every run `failed`: `warm_reagent_plate` failed 3 times; later steps never sent |

Seed 3 failed every run the same way. Seeds 13 and 29 were then chosen to test a
successful retry and a final failure. Both behaved as predicted.

**Final check:** all 26 runs re-checked once the faults had stopped: 26 ok, 0 PROBLEM.
Afterwards all fault settings were reset, and the acceptance script passed 7/7 on both
workflows.

**What we learned:**

- **Wrong about time, never about truth.** Both wrong predictions were about how long
  recovery takes (C4f, C4h), not about what the record said.
- **A driver crash mid-job looks like a hang.** The instrument marks the cut-off job `hung`
  when it restarts (`instrument.go:341`), and replay reports it. The plate reader retries
  it; the liquid handler doesn't. The crashed attempt's end time is the restart, not when
  the work actually stopped. That is the ledger's time, the best evidence there is.
- **One seed gives every run the same faults,** because the roll has no run id. To test a
  particular path, pick the seed from a roll table.

**Follow-up:** no fix needed; no record was false or stuck. K62 was added to the concerns.
