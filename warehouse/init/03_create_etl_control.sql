
-- ============================================================
-- FreshCart Data Warehouse
-- File: 03_create_etl_control.sql
-- Purpose: Track ETL executions, results, and failures
-- ============================================================


-- ============================================================
-- 1. Create ETL Control Schema
-- ============================================================

CREATE SCHEMA IF NOT EXISTS etl_control;


-- ============================================================
-- 2. Create ETL Load Runs Table
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_control.load_runs (

    -- Unique identifier for each ETL execution
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Source being processed
    -- Examples: app.orders, suppliers.fresh, fx_api
    source_name TEXT NOT NULL,

    -- Type of ETL execution
    load_type TEXT NOT NULL,

    -- Target business date (Cairo timezone)
    -- NULL for initial full load
    business_date DATE,

    -- Execution timestamps
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    -- Execution status
    status TEXT NOT NULL DEFAULT 'RUNNING',

    -- Number of rows read from source
    rows_extracted BIGINT NOT NULL DEFAULT 0,

    -- Number of rows actually inserted into Bronze
    rows_loaded BIGINT NOT NULL DEFAULT 0,

    -- Error details in case of failure
    error_message TEXT,


    -- ========================================================
    -- Constraints
    -- ========================================================

    CONSTRAINT chk_load_type
        CHECK (
            load_type IN (
                'FULL',
                'INCREMENTAL',
                'BACKFILL'
            )
        ),

    CONSTRAINT chk_load_status
        CHECK (
            status IN (
                'RUNNING',
                'SUCCESS',
                'FAILED',
                'SKIPPED'
            )
        ),

    CONSTRAINT chk_rows_extracted
        CHECK (rows_extracted >= 0),

    CONSTRAINT chk_rows_loaded
        CHECK (rows_loaded >= 0),

    CONSTRAINT chk_rows_consistency
        CHECK (rows_loaded <= rows_extracted),

    CONSTRAINT chk_finished_at
        CHECK (
            finished_at IS NULL
            OR finished_at >= started_at
        )

);


-- ============================================================
-- 3. Indexes
-- ============================================================

-- Faster lookup by source and business date
CREATE INDEX IF NOT EXISTS idx_load_runs_source_date
ON etl_control.load_runs (
    source_name,
    business_date
);


-- Faster monitoring of failed and running jobs
CREATE INDEX IF NOT EXISTS idx_load_runs_status
ON etl_control.load_runs (
    status
);


-- Faster lookup of recent executions
CREATE INDEX IF NOT EXISTS idx_load_runs_started_at
ON etl_control.load_runs (
    started_at DESC
);
