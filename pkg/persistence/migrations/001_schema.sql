CREATE TABLE IF NOT EXISTS dag_runs (
    id            UUID PRIMARY KEY,
    dag_name      TEXT NOT NULL CHECK (btrim(dag_name) <> ''),
    dag_version   TEXT,
    global_inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'cancelled')),
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ CHECK (finished_at IS NULL OR finished_at >= started_at),
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dag_runs_dag_name ON dag_runs(dag_name);
CREATE INDEX IF NOT EXISTS idx_dag_runs_status ON dag_runs(status);
CREATE INDEX IF NOT EXISTS idx_dag_runs_running ON dag_runs(status) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_dag_runs_name_started ON dag_runs(dag_name, started_at DESC);

CREATE TABLE IF NOT EXISTS task_runs (
    id            UUID PRIMARY KEY,
    dag_run_id    UUID NOT NULL REFERENCES dag_runs(id) ON DELETE CASCADE,
    dag_version   TEXT,
    task_name     TEXT NOT NULL CHECK (btrim(task_name) <> '' AND strpos(task_name, '.') = 0),
    status        TEXT NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed', 'skipped', 'cancelled')),
    attempt       INT NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ CHECK (finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at),
    error_message TEXT,
    description   TEXT NOT NULL DEFAULT '',
    tags          JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority      INT NOT NULL DEFAULT 0,
    order_index   INT NOT NULL CHECK (order_index >= 0),
    run_state_snapshot JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(dag_run_id, task_name)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_dag_run_id ON task_runs(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status);
CREATE INDEX IF NOT EXISTS idx_task_runs_dag_run_status ON task_runs(dag_run_id, status);
CREATE INDEX IF NOT EXISTS idx_task_runs_dag_run_order ON task_runs(dag_run_id, order_index);

CREATE TABLE IF NOT EXISTS task_events (
    id            UUID PRIMARY KEY,
    task_run_id   UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
    event_type    TEXT NOT NULL CHECK (event_type IN ('started', 'succeeded', 'failed', 'retried', 'cancelled', 'skipped', 'retry_exhausted', 'after_hook_failed')),
    attempt       INT NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_events_task_run_id ON task_events(task_run_id);
CREATE INDEX IF NOT EXISTS idx_task_events_event_type ON task_events(event_type);
CREATE INDEX IF NOT EXISTS idx_task_events_task_run_event ON task_events(task_run_id, event_type);
CREATE INDEX IF NOT EXISTS idx_task_events_task_run_attempt ON task_events(task_run_id, attempt);

CREATE TABLE IF NOT EXISTS task_logs (
    id          UUID PRIMARY KEY,
    dag_run_id  UUID NOT NULL REFERENCES dag_runs(id) ON DELETE CASCADE,
    task_run_id UUID REFERENCES task_runs(id) ON DELETE CASCADE,
    level       TEXT NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
    message     TEXT NOT NULL,
    fields      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_logs_dag_run_id ON task_logs(dag_run_id);
CREATE INDEX IF NOT EXISTS idx_task_logs_task_run_id ON task_logs(task_run_id);
CREATE INDEX IF NOT EXISTS idx_task_logs_created_at ON task_logs(created_at DESC);
