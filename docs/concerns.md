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
