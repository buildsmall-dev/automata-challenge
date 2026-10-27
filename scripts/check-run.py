#!/usr/bin/env python3
"""Compare one run's record with the instruments' ledgers and with storage.

    ./scripts/check-run.py <run-id> [<run-id> ...]

For every step and attempt it checks the invariant in CLAUDE.md:
  - every execution in a ledger is an attempt in the record, with the same
    outcome and times, and no job ran twice;
  - a completed step has a completed execution, and a read's object in
    storage has the sha256 the ledger measured;
  - a step that is pending, cancelled or failed claims nothing a ledger
    contradicts; a finished run has nothing left in flight.
Exits 1 if any run has a PROBLEM. Notes are things true but worth seeing.
"""
import hashlib, json, sys, urllib.request
from datetime import datetime

API = "http://localhost:5002"
S3 = "http://localhost:8343"
LEDGERS = {"liquid-handler-1": 7101, "incubator-1": 7102, "plate-reader-1": 7103}
ENDED = {"completed", "failed", "hung"}
QUERY = """query($id: ID!) { run(id: $id) { id status finishedAt steps { name device status phase startedAt finishedAt
  error dataRef attempts { number jobId phase decided outcome startedAt finishedAt data { ledgerSha256 verified } } } } }"""


def get(url, body=None):
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read()


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() if s else None


def close(a, b):
    return a is not None and b is not None and abs(a - b) < 0.001


def check(run_id, ledgers):
    problems, notes = [], []
    body = json.dumps({"query": QUERY, "variables": {"id": run_id}}).encode()
    run = json.loads(get(API + "/graphql", body))["data"]["run"]
    if run is None:
        return [f"run {run_id} not found"], []
    rest = json.loads(get(f"{API}/runs/{run_id}"))
    terminal = run["status"] != "RUNNING" and run["status"] != "PENDING"
    execs = [e for e in ledgers if e.get("run_id") == run_id]
    by_job = {}
    for e in execs:
        by_job.setdefault(e["job_id"], []).append(e)
    for job, es in by_job.items():
        if len(es) > 1:
            problems.append(f"{job} is in the ledger {len(es)} times: it ran more than once")

    last_end = None
    for i, s in enumerate(run["steps"]):
        name, st = s["name"], s["status"]
        r = rest["steps"][i]
        if r["status"].upper() != st or r["started_at"] != s["startedAt"] or r["data_ref"] != s["dataRef"]:
            problems.append(f"{name}: REST and GraphQL disagree")
        mine = {e["job_id"]: e for e in execs if e["step_name"] == name}
        jobs = {a["jobId"] for a in s["attempts"]}
        for job in sorted(set(mine) - jobs):
            problems.append(f"{name}: {job} ran ({mine[job].get('outcome')}) but is not an attempt in the record")
        for a in s["attempts"]:
            e = mine.get(a["jobId"])
            if a["outcome"]:
                if e is None:
                    problems.append(f"{name}/{a['number']}: record says {a['outcome']}, no ledger execution")
                    continue
                if e["outcome"].upper() != a["outcome"]:
                    problems.append(f"{name}/{a['number']}: record {a['outcome']}, ledger {e['outcome']}")
                if not close(ts(a["startedAt"]), e.get("started_at")) or not close(ts(a["finishedAt"]), e.get("finished_at")):
                    problems.append(f"{name}/{a['number']}: times differ from the ledger")
            elif e is not None and e.get("outcome") in ENDED:
                (problems if terminal else notes).append(
                    f"{name}/{a['number']}: ledger says {e['outcome']}, record has no outcome yet")
        decided = [a for a in s["attempts"] if a["decided"]]
        if st == "COMPLETED":
            e = decided and mine.get(decided[0]["jobId"])
            if not e or e.get("outcome") != "completed":
                problems.append(f"{name}: completed without a completed ledger execution")
            elif s["dataRef"] or e.get("data_sha256"):
                ref = s["dataRef"] or ""
                try:
                    got = hashlib.sha256(get(S3 + "/" + ref.removeprefix("s3://"))).hexdigest() if ref else None
                except Exception as err:
                    got = f"unreadable ({err})"
                if got != e.get("data_sha256"):
                    problems.append(f"{name}: data_ref {ref or None} holds {got}, ledger measured {e.get('data_sha256')}")
            if s["finishedAt"] and not close(ts(s["finishedAt"]), e and e.get("finished_at")):
                problems.append(f"{name}: finished_at differs from the decided execution's")
        elif st in ("FAILED", "CANCELLED", "PENDING"):
            ok = [j for j, e in mine.items() if e.get("outcome") == "completed" and not e.get("data_sha256")]
            if ok:
                problems.append(f"{name}: {st} but {ok} completed on the instrument")
            if st == "PENDING" and mine:
                problems.append(f"{name}: pending but the ledger has {sorted(mine)}")
        if st == "RUNNING" and terminal:
            problems.append(f"{name}: still running in a {run['status']} run")
        if s["finishedAt"]:
            last_end = max(last_end or 0, ts(s["finishedAt"]))
        if len(s["attempts"]) > 1 or s["error"]:
            notes.append(f"{name}: {st}/{s['phase']}, {len(s['attempts'])} attempt(s)"
                         + (f", error: {s['error']}" if s["error"] else ""))
    if run["status"] == "COMPLETED" and any(s["status"] != "COMPLETED" for s in run["steps"]):
        problems.append("run completed with a step not completed")
    if terminal and run["finishedAt"] and last_end and not close(ts(run["finishedAt"]), last_end):
        problems.append("run finished_at is not the last step's ledger end")
    return problems, [f"run {run['status']}, {len(execs)} ledger execution(s)"] + notes


def main():
    ledgers = []
    for dev, port in LEDGERS.items():
        try:
            ledgers += json.loads(get(f"http://localhost:{port}/status")).get("executions") or []
        except Exception as err:
            print(f"cannot read {dev}'s ledger: {err}")
            sys.exit(2)
    bad = 0
    for run_id in sys.argv[1:]:
        problems, notes = check(run_id, ledgers)
        print(f"{run_id}: {'PROBLEM' if problems else 'ok'}")
        for n in notes:
            print(f"   note  {n}")
        for p in problems:
            print(f"   PROBLEM {p}")
        bad += bool(problems)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
