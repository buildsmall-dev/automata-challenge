# Concerns

Things we noticed that don't block the current work. Blockers get fixed in the slice
that finds them. These get solved later, or written up in NOTES.md §5 if we don't.

| # | Concern | Where it bites | Solve in / when |
| --- | --- | --- | --- |
| K1 | `error` carries three things: why a step is unresolved, why it failed, and a warning on a completed step (D2). A tool that only reads `status` misses a corruption warning | Scientists and tools reading REST | GraphQL: a separate data-integrity field |
| K2 | A step holds one attempt. Evidence for an earlier job is dropped, not kept as history | Retries: the record can't show what earlier attempts did | Hang/retry `/decide`, then attempt history |
| K3 | A failed outcome with an empty instrument error gives `error: null` | Failed steps without an explanation | When the driver's failure events are built (S4): always send a message |
| K4 | A wrong `DataLost` while an upload is only slow fails the read until a matching `DataStored` arrives | Brief false failure under storage outages | R1 replay slice: send `DataLost` only when the bytes are really gone |
| K5 | A re-read after data loss is a second ledger execution, so acceptance check 5 fails under chaos | Chaos runs look like duplicate runs | NOTES.md: explain it as honest |
| K6 | Operator "declare lost" for a driver that never comes back is deferred (D1) | Runs stuck forever as running | GraphQL mutation, if time allows |
| K7 | `tmp/smoke`, `tmp/exp-*` are Go packages under `./...`, so `go vet ./...` checks them (only `_`-prefixed dirs are skipped) | Local vet/test noise | Rename to `_`-prefixed or delete; not ours to touch |
| K8 | The C1 probe lives only in `tmp/_natsprobe` (local), so the experiment can't be rerun from the repo | Reproducing C1 | Commit it if reviewers ask |
| K9 | Which steps produce data is decided by device name (`plate-reader*`, `store.go` `producesData`) | A new data-producing device would complete without its data being checked | When drivers announce themselves: let them advertise `produces_data` |
| K10 | `steps.lifecycle` is the Go `Step` as JSON with no version. Renaming a field silently drops it from rows already stored | Any later change to `Step` | Before changing `Step`'s fields: add a version or keep old names |
| K11 | The store's guarantee depends on its callers: an event consumer must ack only after `applyToJob` returns nil, and a claim reply must reflect the returned Step | S5: acking early loses facts; replying from a stale read breaks claim/cancel | S5, as part of its check |
| K12 | Each apply locks one row, so there are no deadlocks today. A later transaction that locks several steps, such as a run-wide cancel, must lock them in a fixed order | Run-level operations in S5 / cancel | Lock in `position` order |
| K13 | Device ids go into subjects unchecked: a `.`, `*` or `>` in a device name from `workflows.yaml` breaks or widens subjects | A typo in workflows.yaml | Validate device names when workflows load |
| K14 | No rolling-upgrade story: a v2 driver against a v1 executor has every message rejected (fails safe, not zero-downtime) | Deploys | Out of scope; NOTES.md |
| K15 | An event's `device` should equal the device in its subject | A mis-wired driver reporting for another device | Done in S5a, and the step's assigned device must match too (Codex) |
| K16 | (Codex, S4) If the instrument's end-of-job ledger write fails, `Status()` says completed and we publish it, but after a restart the ledger file says hung (`instrument.go:241`). The record would then disagree with the ledger | Needs a disk error; no chaos tool causes it | Accepted; NOTES.md §5. We cannot see a failed ledger write from outside the instrument |
| K17 | (Codex, S4) The driver drops the plate reader's `Outcome.Data`: a read finishes but is never stored, so it stays `storing` | Any plate read until S6 | Done in S6 (upload, then `stored`); `data_lost` moves to R1 (K32) |
| K18 | (Codex, S4) A driver restart between the ledger outcome and the acked `finished` publish loses that event; a repeated command then gets `known` and nothing reports it | Driver crash mid-report | **R1** (planned): replay ledger outcomes on start, and on `known` |
| K19 | A claim granted but `Run` returns `ErrBusy` (instrument busy with a job we did not start): the step stays claimed and nothing ran | Rare: an instrument busy outside our knowledge | **S7**: reconcile finds no ledger entry and returns the step to dispatched |
| K20 | The driver calls `inst.Status()` (a copy of the whole ledger) on every command and every 50ms while a job runs | Long-lived ledgers with many thousands of entries | Fine at this scale; look up by job id if it shows |
| K21 | `stored` events are Nak'd every 30s until S6 handles them | None today: no driver publishes `stored` yet | Done in S6 |
| K22 | `LAB_EVENTS` keeps everything (no `MaxAge`/limits) | Disk on a long-lived NATS | Set retention once replay needs (R1) are known |
| K23 | A Term'd (malformed) event is only logged; there is no dead-letter place to inspect it later | Debugging a misbehaving driver | Advisory or a DLQ stream if it shows up |
| K24 | Claims are handled one at a time per executor (one subscription); a slow database delays every claim, and drivers keep asking | DB latency spikes | Fine at three devices; worker pool if needed |
| K25 | A step claimed but never started (driver died after the claim), or hung, keeps its run `running` forever | Driver crashes, instrument hangs | **S7** reconcile / hang-retry `/decide` |
| K26 | A late `DataStored` after `DataLost` turns a failed read into completed, after its run was already finished as failed | Storage slow enough that the driver gave up on it | Re-evaluate a finished run when a step changes, or freeze evidence once a run ends |
| K27 | Steps after a failed one stay `pending` in a failed run (never sent, so true, but a reader may expect `cancelled`) | API/GraphQL readers | GraphQL: say why a step never ran |
| K28 | Every 500ms the loop rescans all running runs, one query per run | Many concurrent runs | Fine at this scale; one joined query or LISTEN/NOTIFY if it shows |
| K29 | Runs that share a device only retry on `busy`; no fairness or queueing across runs | Several runs at once on one instrument | Out of scope unless chaos shows starvation |
| K30 | While storage is down the plate reader is held (busy) until its bytes are stored; a long outage stops all reads | Storage outages | Intended: one copy in memory, never dropped. Chaos-test it; a spool to disk if it bites |
| K31 | A `stored` event whose object is missing or unreadable is checked again every 2s, for ever (e.g. the object was deleted) | Storage loses an object | The step stays `running`, which is true; S7/operator can resolve |
| K32 | A driver restart between a read and its upload loses the bytes; the read stays `running` for ever. Seen on the stack: `run-8c366ff2ecb6` (S5b run, driver rebuilt) | Driver crashes, every redeploy mid-read | **R1**: on start, for each completed read in the ledger: object present -> `stored` (Codex: a `Put` can succeed and the restart lose only the `stored` publish), absent -> `data_lost` |
| K33 | Each redelivery of a `stored` event adds another storage check to the step (`Checks` grows) | Only when an ack is lost after the commit | Fine; dedupe checks by SHA if it grows |
| K34 | (Codex, S6) A verified read whose object later disappears still says `completed` with a `data_ref` to nothing. A redelivered `stored` (lost ack) that then fails to read back only retries | Objects deleted after verification | D2-style warning from a periodic re-check of completed reads' objects; not on the event path |
