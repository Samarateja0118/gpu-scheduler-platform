CREATE TABLE IF NOT EXISTS tenants (
    name       TEXT PRIMARY KEY,
    guaranteed INT  NOT NULL CHECK (guaranteed >= 0),
    max_gpus   INT  NOT NULL CHECK (max_gpus >= guaranteed),
    pool       TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS nodes (
    name       TEXT PRIMARY KEY,
    gpu_type   TEXT NOT NULL,
    gpus       INT  NOT NULL CHECK (gpus > 0),
    pool       TEXT NOT NULL DEFAULT '',
    cordoned   BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    tenant          TEXT NOT NULL,
    priority        INT  NOT NULL,
    gpus            INT  NOT NULL CHECK (gpus > 0),
    gpu_type        TEXT NOT NULL DEFAULT '',
    runtime_seconds DOUBLE PRECISION NOT NULL,
    preemptible     BOOLEAN NOT NULL,
    state           TEXT NOT NULL,
    node            TEXT NOT NULL DEFAULT '',
    gpu_indices     INT[] NOT NULL DEFAULT '{}',
    borrowed        BOOLEAN NOT NULL DEFAULT false,
    preemptions     INT NOT NULL DEFAULT 0,
    requeues        INT NOT NULL DEFAULT 0,
    message         TEXT NOT NULL DEFAULT '',
    submitted_at    TIMESTAMPTZ NOT NULL,
    queued_at       TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    wait_seconds    DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_active_idx ON jobs (state) WHERE state IN ('Pending', 'Running');
CREATE INDEX IF NOT EXISTS jobs_tenant_idx ON jobs (tenant, submitted_at DESC);

CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL PRIMARY KEY,
    at           TIMESTAMPTZ NOT NULL,
    type         TEXT NOT NULL,
    job_id       TEXT,
    node         TEXT NOT NULL DEFAULT '',
    by_job       TEXT NOT NULL DEFAULT '',
    lost_seconds DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS events_job_idx ON events (job_id, at);
