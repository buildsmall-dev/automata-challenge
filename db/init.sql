-- Lab automation: workflow execution.
--
-- A starting point, not a design. The schema is yours: change anything.

CREATE DATABASE executor_db;

\c executor_db

-- One execution of a workflow.
CREATE TABLE runs (
    id            TEXT PRIMARY KEY,
    workflow_name TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ
);

-- One node of the workflow DAG, for one run.
--
-- lifecycle is the source of truth: the Step from services/executor/lifecycle.go,
-- changed only by Apply inside a transaction that holds this row's lock. phase,
-- job_id and status..data_ref are derived from it in that same transaction:
-- phase and job_id for lookups, the rest is what the API reports.
CREATE TABLE steps (
    run_id      TEXT        NOT NULL REFERENCES runs(id),
    name        TEXT        NOT NULL,
    position    INT         NOT NULL,
    device_id   TEXT        NOT NULL,
    depends_on  TEXT[]      NOT NULL DEFAULT '{}',
    lifecycle   JSONB       NOT NULL,
    phase       TEXT        NOT NULL DEFAULT 'pending',
    job_id      TEXT        UNIQUE,
    status      TEXT        NOT NULL DEFAULT 'pending',
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error       TEXT,
    data_ref    TEXT,
    PRIMARY KEY (run_id, name)
);
