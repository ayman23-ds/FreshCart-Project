import os
from pathlib import Path
import psycopg

MIGRATIONS_DIR = Path("/app/migrations")

def run_migrations():
    with psycopg.connect(
        host=os.environ["WAREHOUSE_HOST"],
        port=os.getenv("WAREHOUSE_PORT", "5432"),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    ) as conn:

        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS etl_control
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl_control.schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                version = path.stem

                cur.execute(
                    "SELECT 1 FROM etl_control.schema_migrations WHERE version = %s",
                    (version,)
                )

                if cur.fetchone():
                    print(f"SKIPPED: {version}", flush=True)
                    continue

                cur.execute(path.read_text(encoding="utf-8"))

                cur.execute(
                    """
                    INSERT INTO etl_control.schema_migrations (version)
                    VALUES (%s)
                    """,
                    (version,)
                )

                print(f"APPLIED: {version}", flush=True)

    print("Migrations completed successfully.", flush=True)


if __name__ == "__main__":
    run_migrations()