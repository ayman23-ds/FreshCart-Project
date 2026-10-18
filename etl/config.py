
# ============================================================
# FreshCart ETL - Configuration
# File: etl/config.py
# ============================================================

import os
from pathlib import Path
from dotenv import load_dotenv


# 1. Find the project root directory
BASE_DIR = Path(__file__).resolve().parent.parent


# 2. Load variables from .env
load_dotenv(BASE_DIR / ".env")


# 3. Read required environment variables
def required_env(name):
    value = os.getenv(name)

    if not value:
        raise ValueError(
            f"Missing environment variable: {name}"
        )

    return value


# 4. Source Database (READ ONLY)
SOURCE_DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "freshcart_source",
    "user": required_env("SOURCE_READER_USER"),
    "password": required_env("SOURCE_READER_PASSWORD"),
}


# 5. Warehouse Database (READ / WRITE)
WAREHOUSE_DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "freshcart_warehouse",
    "user": "warehouse_user",
    "password": required_env("WAREHOUSE_DB_PASSWORD"),
}


# 6. General ETL Settings
BUSINESS_TIMEZONE = "Africa/Cairo"

SUPPLIER_DIR = BASE_DIR / "source_files" / "suppliers"
