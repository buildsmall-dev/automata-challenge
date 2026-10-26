---
name: break
description: Run one chaos experiment against the lab automation stack. Predict first, inject the fault, compare the run record with the instrument ledgers, and log it in docs/chaos-log.md. Use when the user says /break, or to test whether a slice holds under faults.
---

# /break <fault>

An experiment, not poking around. The prediction comes before the run.

## Steps

1. **Pick the fault and the moment.** For example, "disconnect plate-reader-1 for 10s
   while read_plate is running", "kill executor right after combine is dispatched",
   or "INC_HANG_PCT=50 with CHAOS_SEED=3". Use README "Breaking things" for the tools.
   Record seeds and env vars so it can be repeated.

2. **Predict, and write it down before running.** What will the run record say,
   what will the ledgers say, and will they agree? Say which code path you expect
   to handle it, or that nothing handles it yet.

3. **Run it.** Start from a known state (stack up, instrument fault env vars as
   intended). Create and start a run, then inject the fault at the chosen moment.
   Let the run settle (terminal status or a clear stall), and capture:
   - `curl -s localhost:5002/runs/<id> | jq` and `/timeline`;
   - each instrument's executions for this run:
     `curl -s localhost:710N/status | jq '.executions[] | select(.run_id=="<id>")'`;
   - for plate reader steps: does the `data_ref` object exist, and does its sha256
     match the ledger's `data_sha256`?
   - relevant `docker compose logs`, kept short.

4. **Compare** against the invariant in `CLAUDE.md`. For each step, does the record
   agree with the ledger (status, number of executions, times)? Is every
   `completed` read backed by a stored object with the right hash? Does any step
   claim success where the evidence says `hung` or is missing? Is anything stuck? Was the prediction right? A wrong
   prediction is a finding about our understanding, so say what we misunderstood.

5. **Log it** in `docs/chaos-log.md` with the template there.

6. **If the record was false or stuck**, propose the fix as a `/slice`, with this
   experiment as its check. Don't fix it inside `/break`.

Before any `acceptance.sh` run afterwards, set the instrument fault percentages back to 0.
