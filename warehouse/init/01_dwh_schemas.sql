-- =============================================================================
-- FreshCart source DWH : Schemas definitions
-- This file runs automatically the first time the warehouse container starts.
-- =============================================================================

-- Bronze: Raw source data
CREATE SCHEMA IF NOT EXISTS bronze_app;
CREATE SCHEMA IF NOT EXISTS bronze_suppliers;
CREATE SCHEMA IF NOT EXISTS bronze_fx;

-- Silver: Cleaned data, built by dbt
CREATE SCHEMA IF NOT EXISTS silver_app;
CREATE SCHEMA IF NOT EXISTS silver_suppliers;
CREATE SCHEMA IF NOT EXISTS silver_fx;

-- Gold: Analytics models, built by dbt
CREATE SCHEMA IF NOT EXISTS gold;

