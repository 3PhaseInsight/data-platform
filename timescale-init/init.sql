-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

---------------------------------------------
----------------- USERS ---------------------
---------------------------------------------

-- Airflow User (make sure to change password)
CREATE ROLE airflow_user LOGIN PASSWORD 'superStrongPassword';
GRANT CONNECT ON DATABASE "3phi-db" TO airflow_user;
CREATE SCHEMA IF NOT EXISTS airflow AUTHORIZATION airflow_user;
-- Keep search_path minimal so Alembic works where we expect
ALTER ROLE airflow_user SET search_path = airflow;

-- Application User (change password)
CREATE USER threephi_db_user WITH PASSWORD 'appPassword';
GRANT CONNECT ON DATABASE "3phi-db" TO threephi_db_user;
GRANT USAGE, CREATE ON SCHEMA public TO threephi_db_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO threephi_db_user;

---------------------------------------------
----------------- TABLES --------------------
---------------------------------------------

-- Create the workflow_states table
CREATE TABLE IF NOT EXISTS workflow_states (
    id SERIAL PRIMARY KEY,
    workflow TEXT UNIQUE NOT NULL,
    completed bool DEFAULT FALSE,
    description TEXT,
    updated_at TIMESTAMP DEFAULT now()
);

-- Trigger to auto-update `updated_at`
CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_workflow_states_timestamp
    BEFORE UPDATE ON workflow_states
    FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

---------------------------------------------
--- ADDITIONS FOR REWORKED STORAGE LAYOUT ---
---------------------------------------------

CREATE TABLE IF NOT EXISTS ingest_batch (
    id              UUID PRIMARY KEY,
    source_file     TEXT NOT NULL,                      -- original CSV path/name
    received_at     timestamptz NOT NULL DEFAULT now(), -- when first seen
    status          TEXT NOT NULL,                      -- in_progress | processed | failed
    error_log       TEXT,                               -- optional error details
    stats_json      JSONB,                              -- counts, null rates, etc.
    run_id          TEXT,                               -- optional logical run key (e.g., Airflow run)
    UNIQUE (source_file, run_id)                        -- idempotency guard (tune as needed)
);

ALTER TABLE ingest_batch
    ADD CONSTRAINT ingest_batch_status_chk
        CHECK (status IN ('in_progress','processed','failed'));

-- Create file index table
CREATE TABLE IF NOT EXISTS file_index (
    id UUID PRIMARY KEY,
    s3_key TEXT NOT NULL,                           -- filepath on s3
    dt DATE NOT NULL,                               -- partitioning "day"
    shard INT NOT NULL,                             -- shard number
    seq BIGINT NOT NULL,                            -- sequence number within (dt, shard) for traversal
    ts_start timestamptz NOT NULL,                  -- min ts in this file
    ts_end timestamptz NOT NULL,                    -- max ts in this file
    rows BIGINT NOT NULL,                           -- row count of file
    bytes BIGINT NOT NULL,                          -- file size in bytes
    schema_version TEXT NOT NULL,                   -- schema version file was written with
    status TEXT NOT NULL,                           -- lifecycle state, staged/ready/deprecated/failed
    batch_id UUID NOT NULL,                         -- link to batch table
    ingest_file TEXT NOT NULL,                      -- origin file name
    created_at timestamptz NOT NULL DEFAULT now(),  -- ts of file ingestion
    committed_at timestamptz                        -- ts of promotion from staging -> ready
);

-- enforce allowed statuses
ALTER TABLE file_index
    ADD CONSTRAINT file_index_status_chk
        CHECK (status IN ('staged','ready','deprecated','failed'));

-- Ensure seq is unique within a ring (dt, shard)
ALTER TABLE file_index
    ADD CONSTRAINT file_index_unique_ring_seq
        UNIQUE (dt, shard, seq);

-- Foreign Key to ingest batch
ALTER TABLE file_index
  ADD CONSTRAINT file_index_batch_fk
  FOREIGN KEY (batch_id) REFERENCES ingest_batch(id) ON DELETE SET NULL;

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_file_ready_dt_shard_seq
    ON file_index (dt, shard, seq DESC)
    WHERE status = 'ready';

CREATE INDEX IF NOT EXISTS idx_file_ready_time_prune
    ON file_index (dt, ts_start, ts_end)
    WHERE status = 'ready';

CREATE INDEX IF NOT EXISTS idx_file_ready_schema
    ON file_index (schema_version)
    WHERE status = 'ready';

-- Table to track which smart meters exist in the dataset
CREATE TABLE IF NOT EXISTS meter (
    id          TEXT PRIMARY KEY,                   -- unique smart meter identifier
    first_seen  timestamptz NOT NULL,               -- earliest timestamp with data for this meter
    last_seen   timestamptz NOT NULL,               -- latest timestamp with data for this meter
    total_rows  BIGINT NOT NULL DEFAULT 0,          -- cumulative row count across all files
    updated_at  timestamptz NOT NULL DEFAULT now()  -- last time this row was updated
);