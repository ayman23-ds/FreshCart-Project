
# ============================================================
# FreshCart ETL - PostgreSQL Source to Bronze
# File: etl/app_loader.py
#
# Supports:
#   FULL
#   INCREMENTAL
#   BACKFILL
#   Batch extraction and loading
#   Idempotent inserts
#   ETL run logging
#   Progress and execution time
# ============================================================

import time
from datetime import date, datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

from psycopg import sql

from etl.db import (
    get_source_connection,
    get_warehouse_connection,
)


# ============================================================
# 1. Configuration
# ============================================================

SOURCE_SCHEMA = "app"
BRONZE_SCHEMA = "bronze_app"

CAIRO_TZ = ZoneInfo("Africa/Cairo")

DEFAULT_BATCH_SIZE = 5000

# Identify keys used to prevent repeatition
TABLE_KEYS = {
    "stores": ["store_id", "updated_at"],
    "customers": ["customer_id", "updated_at"],
    "products": ["product_id", "updated_at"],
    "orders": ["order_id", "updated_at"],
    "order_items": ["order_item_id", "updated_at"],
    "payments": ["payment_id", "updated_at"],
    "product_prices": ["product_id","valid_from","updated_at",],
}


# ============================================================
# 2. Validation
# ============================================================

# Check Table one of our tables
def validate_table(table_name):

    if table_name not in TABLE_KEYS:
        raise ValueError(
            f"Unsupported source table: {table_name}"
        )


# Check Type of Loading
def validate_load_type(load_type):

    allowed = {"FULL", "INCREMENTAL", "BACKFILL"}

    if load_type not in allowed:
        raise ValueError(
            f"Invalid load type: {load_type}"
        )


# ============================================================
# 3. Cairo Business-Day Window
# ============================================================

def get_day_window(business_date):

    if isinstance(business_date, str):
        business_date = date.fromisoformat(
            business_date
        )

    if not isinstance(business_date, date) or isinstance(
        business_date, datetime
    ):
        raise ValueError(
            "business_date must be YYYY-MM-DD or a date"
        )

    start = datetime.combine(
        business_date,
        dt_time.min,
        tzinfo=CAIRO_TZ,
    )

    end = datetime.combine(
        business_date + timedelta(days=1),
        dt_time.min,
        tzinfo=CAIRO_TZ,
    )

    return business_date, start, end


# ============================================================
# 4. ETL Run Logging
# ============================================================

# Document Start Loading Process and provide logs about it then return ID identifier of the process
def start_load_run(
    table_name,
    load_type,
    business_date=None,
):
    # start connection with WH 
    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:

            # Execute SQl Commands
            cur.execute(
                """
                INSERT INTO etl_control.load_runs (
                    source_name,
                    load_type,
                    business_date,
                    status
                )
                VALUES (%s, %s, %s, 'RUNNING')
                RETURNING run_id
                """,
                (
                    f"app.{table_name}",
                    load_type,
                    business_date,
                ),
            )

            run_id = cur.fetchone()[0]

    return run_id

# Modify Logs by insert some extra info after finishing the process
def finish_load_run(
    run_id,
    status,
    rows_extracted,
    rows_loaded,
    error_message=None,
):

    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE etl_control.load_runs
                SET
                    finished_at = NOW(),
                    status = %s,
                    rows_extracted = %s,
                    rows_loaded = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (
                    status,
                    rows_extracted,
                    rows_loaded,
                    error_message,
                    run_id,
                ),
            )


# ============================================================
# 5. Extract Data in Batches
# ============================================================

# Read Data as batches "Read Only"
def extract_batches(
    table_name,
    batch_size=DEFAULT_BATCH_SIZE,
    updated_from=None,
    updated_until=None,
):
    # Check Table name 
    validate_table(table_name)
    
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    # Create name of table as it is in DB "EX: table_identifier = app.orders "
    table_identifier = sql.Identifier(SOURCE_SCHEMA,table_name,)
    
    query = sql.SQL("SELECT * FROM {}").format(table_identifier)

    params = []

    # used in incremental load 
    if updated_from is not None:
        query += sql.SQL(" WHERE updated_at >= %s")
        params.append(updated_from)

        if updated_until is not None:
            query += sql.SQL( " AND updated_at < %s")
            params.append(updated_until)

    elif updated_until is not None:
        raise ValueError("updated_until requires updated_from")

    with get_source_connection() as conn:

        # Consistent snapshot while extracting the table
        conn.execute(
            "SET TRANSACTION ISOLATION LEVEL "
            "REPEATABLE READ READ ONLY"
        )

        with conn.cursor( name="freshcart_source_cursor") as cur:
            # Execute query
            cur.execute(query, params)
            # Retrive cols names to be inserted correctly to same cols in warehouse
            columns = [column.name for column in cur.description]

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                yield columns, rows


# ============================================================
# 6. Load One Batch into Bronze
# ============================================================

def load_batch(
    table_name,
    rows,
    columns,
):

    validate_table(table_name)

    if not rows:
        return 0

    if len(columns) != len(rows[0]):
        raise ValueError("Column count does not match row width")

    conflict_columns = TABLE_KEYS[table_name]

    query = sql.SQL("""
        INSERT INTO {} ({})
        VALUES ({})
        ON CONFLICT ({})
        DO NOTHING
    """).format(
        
        # table name
        sql.Identifier(BRONZE_SCHEMA,table_name,),

        # cols
        sql.SQL(", ").join(
            sql.Identifier(col)
            for col in columns
        ),

        # placeholder for values
        sql.SQL(", ").join(
            sql.Placeholder()
            for _ in columns
        ),

        # cnoflict
        sql.SQL(", ").join(
            sql.Identifier(col)
            for col in conflict_columns
        ),
    )

    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:

            cur.executemany(query, rows)

            inserted = cur.rowcount

    return inserted


# ============================================================
# 7. Main Table Loader
# ============================================================

def load_table(
    table_name,
    load_type="FULL",
    business_date=None,
    batch_size=DEFAULT_BATCH_SIZE,
):

#----- Validations------
    load_type = load_type.upper()

    validate_table(table_name)
    validate_load_type(load_type)

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    updated_from = None
    updated_until = None

    if load_type in {"INCREMENTAL", "BACKFILL"}:

        if business_date is None:
            raise ValueError(
                "business_date is required for "
                "INCREMENTAL and BACKFILL"
            )

        business_date, updated_from, updated_until = (
            get_day_window(business_date)
        )

    elif business_date is not None:
        raise ValueError("FULL load must not specify business_date")

    print()
    print("=" * 60)
    print(f"Table       : {table_name}")
    print(f"Load type   : {load_type}")
    print(f"Business day: {business_date}")
    print(f"Batch size  : {batch_size:,}")
    print("=" * 60)
#----- Validations ends------

#----- start clock ------

    started = time.perf_counter()

    rows_extracted = 0
    rows_loaded = 0
    batch_number = 0

    # Register the ETL attempt in load runs
    run_id = start_load_run(
        table_name,
        load_type,
        business_date,
    )

    try:

        # Read and load one batch at a time
        for columns, rows in extract_batches(
            table_name=table_name,
            batch_size=batch_size,
            updated_from=updated_from,
            updated_until=updated_until,
        ):

            batch_number += 1

            inserted = load_batch(
                table_name=table_name,
                rows=rows,
                columns=columns,
            )

            rows_extracted += len(rows)
            rows_loaded += inserted

            elapsed = time.perf_counter() - started

            print(
                f"Batch {batch_number:,} | "
                f"Extracted: {rows_extracted:,} | "
                f"Inserted: {rows_loaded:,} | "
                f"Time: {elapsed:.1f}s"
            )

        # Mark success only after all batches complete
        finish_load_run(
            run_id=run_id,
            status="SUCCESS",
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
        )

        elapsed = time.perf_counter() - started

        print("-" * 60)
        print(f"SUCCESS: {table_name}")
        print(f"Extracted : {rows_extracted:,}")
        print(f"Inserted  : {rows_loaded:,}")
        print(f"Skipped   : {rows_extracted - rows_loaded:,}")
        print(f"Duration  : {elapsed:.2f} seconds")
        print(f"Run ID    : {run_id}")
        print("=" * 60)

        return {
            "run_id": run_id,
            "table": table_name,
            "load_type": load_type,
            "business_date": business_date,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
            "duration_seconds": elapsed,
            "status": "SUCCESS",
        }
    
    # If errors occured
    except Exception as exc:

        elapsed = time.perf_counter() - started

        try:
            finish_load_run(
                run_id=run_id,
                status="FAILED",
                rows_extracted=rows_extracted,
                rows_loaded=rows_loaded,
                error_message=str(exc)[:4000],
            )
        except Exception as logging_error:
            print("Could not update ETL run status:",logging_error,)

        print(f"FAILED: {table_name}")
        print(f"Error: {exc}")
        print(f"Duration: {elapsed:.2f} seconds")

        raise





def load_all_tables(
    load_type="FULL",
    business_date=None,
    batch_size=5000
):

    tables = [
        "stores",
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "product_prices",
    ]

    results = []

    for table_name in tables:

        result = load_table(
            table_name=table_name,
            load_type=load_type,
            business_date=business_date,
            batch_size=batch_size,
        )

        results.append(result)

    return results
