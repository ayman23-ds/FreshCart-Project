
# ============================================================
# FreshCart ETL - Database Connections
# File: etl/db.py
# ============================================================

import psycopg

from etl.config import (
    SOURCE_DB_CONFIG,
    WAREHOUSE_DB_CONFIG
)


# ============================================================
# 1. Source Database Connection (READ ONLY)
# ============================================================

def get_source_connection():

    conn = psycopg.connect(
        **SOURCE_DB_CONFIG,
        connect_timeout=10,
        options="-c default_transaction_read_only=on"
    )

    return conn


# ============================================================
# 2. Warehouse Database Connection (READ / WRITE)
# ============================================================

def get_warehouse_connection():

    conn = psycopg.connect(
        **WAREHOUSE_DB_CONFIG,
        connect_timeout=10
    )

    return conn
