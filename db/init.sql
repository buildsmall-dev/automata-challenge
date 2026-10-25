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
CREATE TABLE steps (
    run_id      TEXT        NOT NULL REFERENCES runs(id),
    name        TEXT        NOT NULL,
    position    INT         NOT NULL,
    device_id   TEXT        NOT NULL,
    depends_on  TEXT[]      NOT NULL DEFAULT '{}',
    status      TEXT        NOT NULL DEFAULT 'pending',
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error       TEXT,
    data_ref    TEXT,
    PRIMARY KEY (run_id, name)
);
