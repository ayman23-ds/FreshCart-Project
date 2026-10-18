
# ============================================================
# FX API Loader - Step 1: Fetch API Response
# ============================================================

import os
import time
from datetime import date

import requests
from dotenv import load_dotenv

from psycopg.types.json import Jsonb
from etl.db import get_warehouse_connection

from datetime import date, timedelta
# ============================================================
# 1. Configuration
# ============================================================

load_dotenv()

FX_API_BASE_URL = os.getenv(
    "FX_API_BASE_URL",
    "http://localhost:8000",
).rstrip("/")

FX_API_KEY = os.getenv("FX_API_KEY")

DEFAULT_TIMEOUT = 15
MAX_RETRIES = 3


# ============================================================
# 2. Fetch FX Rates for One Date
# ============================================================

def fetch_fx_rates(
    business_date,
    max_retries=MAX_RETRIES,
    timeout=DEFAULT_TIMEOUT,
):

    if isinstance(business_date, date):
        business_date = business_date.isoformat()

    # Validate date
    business_date = date.fromisoformat(
        business_date
    ).isoformat()

    if not FX_API_KEY:
        raise ValueError(
            "FX_API_KEY is missing from environment"
        )

    url = f"{FX_API_BASE_URL}/v1/rates"

    headers = {
        "X-API-Key": FX_API_KEY,
    }

    params = {
        "date": business_date,
    }

    # Retry temporary failures
    for attempt in range(1, max_retries + 1):

        try:

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
            )

            response.raise_for_status()

            data = response.json()

            if not isinstance(data, dict):
                raise ValueError(
                    "Expected JSON object from FX API"
                )

            return data

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as error:

            if attempt == max_retries:
                raise

            wait_seconds = 2 ** (attempt - 1)

            print(
                f"Temporary API error: {error}. "
                f"Retrying in {wait_seconds}s..."
            )

            time.sleep(wait_seconds)

        except requests.exceptions.HTTPError:

            status_code = response.status_code

            # Retry rate limits and server errors
            if (
                status_code == 429
                or 500 <= status_code < 600
            ) and attempt < max_retries:

                wait_seconds = 2 ** (attempt - 1)

                print(
                    f"HTTP {status_code}. "
                    f"Retrying in {wait_seconds}s..."
                )

                time.sleep(wait_seconds)

            else:
                raise




# ============================================================
# 4. Fetch FX History (Maximum 31 Days)
# ============================================================

def fetch_fx_history(
    start_date,
    end_date,
    max_retries=MAX_RETRIES,
    timeout=DEFAULT_TIMEOUT,
):

    start_date = date.fromisoformat(str(start_date))
    end_date = date.fromisoformat(str(end_date))

    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")

    if (end_date - start_date).days + 1 > 31:
        raise ValueError("Maximum 31 days per history request")

    if not FX_API_KEY:
        raise ValueError("FX_API_KEY is missing")

    url = f"{FX_API_BASE_URL}/v1/rates/history"

    headers = {"X-API-Key": FX_API_KEY}

    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
            )

            response.raise_for_status()
            data = response.json()

            if (
                not isinstance(data, dict)
                or not isinstance(data.get("days"), list)
            ):
                raise ValueError("Unexpected FX history response")

            return data

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as error:

            if attempt == max_retries:
                raise

            wait_seconds = 2 ** (attempt - 1)
            print(f"Temporary error: {error}")
            time.sleep(wait_seconds)

        except requests.exceptions.HTTPError:

            status_code = response.status_code

            if (
                status_code == 429
                or 500 <= status_code < 600
            ) and attempt < max_retries:

                wait_seconds = 2 ** (attempt - 1)
                print(f"HTTP {status_code}. Retrying...")
                time.sleep(wait_seconds)

            else:
                raise



# ============================================================
# 5. Historical FX Backfill
# ============================================================


def backfill_fx_rates(start_date, end_date):

    start_date = date.fromisoformat(str(start_date))
    end_date = date.fromisoformat(str(end_date))

    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")

    run_id = start_fx_run("BACKFILL", start_date)

    summary = {
        "requests": 0,
        "days_received": 0,
        "responses_loaded": 0,
        "responses_skipped": 0,
    }

    current_date = start_date

    try:
        while current_date <= end_date:

            chunk_end = min(
                current_date + timedelta(days=30),
                end_date,
            )

            print(
                f"\nFetching FX history: "
                f"{current_date} -> {chunk_end}"
            )

            data = fetch_fx_history(
                current_date,
                chunk_end,
            )

            summary["requests"] += 1
            summary["days_received"] += len(data["days"])

            with get_warehouse_connection() as conn:
                with conn.cursor() as cur:

                    cur.execute("""
                        INSERT INTO bronze_fx.fx_rates (
                            response_data
                        )
                        VALUES (%s)
                        ON CONFLICT (response_data)
                        DO NOTHING
                    """, (Jsonb(data),))

                    if cur.rowcount == 1:
                        summary["responses_loaded"] += 1
                    else:
                        summary["responses_skipped"] += 1

            print(
                f"Days received: {len(data['days'])} | "
                f"Responses loaded: {summary['responses_loaded']}"
            )

            current_date = chunk_end + timedelta(days=1)

        status = (
            "SUCCESS"
            if summary["responses_loaded"] > 0
            else "SKIPPED"
        )

        finish_fx_run(
            run_id=run_id,
            status=status,
            rows_extracted=summary["requests"],
            rows_loaded=summary["responses_loaded"],
        )

        print(f"\nRun ID: {run_id}")
        print(f"Status: {status}")
        print("FX BACKFILL SUMMARY")

        for key, value in summary.items():
            print(f"{key}: {value}")

        return summary

    except Exception as error:

        finish_fx_run(
            run_id=run_id,
            status="FAILED",
            rows_extracted=summary["requests"],
            rows_loaded=summary["responses_loaded"],
            error_message=str(error),
        )

        raise



# ============================================================
# 3. Load FX Rates into Bronze
# ============================================================

# ============================================================
# Daily FX Loader with Run Tracking
# ============================================================

def load_fx_rates(business_date):

    business_date = date.fromisoformat(str(business_date))

    run_id = start_fx_run(
        load_type="INCREMENTAL",
        business_date=business_date,
    )

    rows_extracted = 0
    rows_loaded = 0

    try:
        # 1. Fetch FX API response
        data = fetch_fx_rates(business_date)
        rows_extracted = 1

        # 2. Insert raw response into Bronze
        with get_warehouse_connection() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    INSERT INTO bronze_fx.fx_rates (
                        response_data
                    )
                    VALUES (%s)
                    ON CONFLICT (response_data)
                    DO NOTHING
                """, (Jsonb(data),))

                rows_loaded = cur.rowcount

        # 3. Determine status
        status = (
            "SUCCESS"
            if rows_loaded > 0
            else "SKIPPED"
        )

        # 4. Finish ETL run
        finish_fx_run(
            run_id=run_id,
            status=status,
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
        )

        print(
            f"FX {business_date} | "
            f"Run ID: {run_id} | "
            f"Status: {status} | "
            f"Rows loaded: {rows_loaded}"
        )

        return {
            "run_id": run_id,
            "business_date": str(business_date),
            "status": status,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
        }

    except Exception as error:

        finish_fx_run(
            run_id=run_id,
            status="FAILED",
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
            error_message=str(error),
        )

        raise


# ============================================================
# FX ETL - Run Tracking
# ============================================================

def start_fx_run(load_type, business_date=None):

    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO etl_control.load_runs (
                    source_name,
                    load_type,
                    business_date,
                    started_at,
                    status
                )
                VALUES (
                    'FX_API',
                    %s,
                    %s,
                    NOW(),
                    'RUNNING'
                )
                RETURNING run_id
            """, (load_type, business_date))

            run_id = cur.fetchone()[0]

    return run_id


def finish_fx_run(
    run_id,
    status,
    rows_extracted,
    rows_loaded,
    error_message=None,
):

    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                UPDATE etl_control.load_runs
                SET
                    finished_at = NOW(),
                    status = %s,
                    rows_extracted = %s,
                    rows_loaded = %s,
                    error_message = %s
                WHERE run_id = %s
            """, (
                status,
                rows_extracted,
                rows_loaded,
                error_message,
                run_id,
            ))
