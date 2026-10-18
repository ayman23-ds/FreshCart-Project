"""FreshCart supplier CSV -> Bronze: extract, load, orchestrate (FULL/INCREMENTAL/BACKFILL).

Place this file at etl/supplier_loader.py in the project root.
Requires existing bronze_suppliers tables and etl_control.load_runs.
"""
import csv
import logging
from datetime import date, datetime
from pathlib import Path

from psycopg import sql
from etl.db import get_warehouse_connection


#################################################################################################################
#                                                   CONFIGURATION
#################################################################################################################
logger = logging.getLogger(__name__)
SUPPLIER_DIR = Path(__file__).resolve().parent.parent / "source_files" / "suppliers"
SUPPLIERS = {
    "FRESH": {
        "folder": "fresh", 
        "pattern": "fresh_stock_*.csv",
        "delimiter": ",",
        "table": "fresh_stock",
        "date_format": "%Y-%m-%d",
        "prefix": "fresh_stock_",
        "columns": ["supplier", "product_id", "store_id", "stock_date", "quantity_on_hand", "reorder_level"],
    },
        
    "GENERAL": {
        "folder": "general",
        "pattern": "GENERAL_STOCK_*.csv",
        "delimiter": ";",
        "table": "general_stock",
        "date_format": "%Y%m%d",
        "prefix": "GENERAL_STOCK_",
        "columns": [
            "Supplier", "Product", "Store",
            "Date", "Qty", "MinQty"
        ],
        "alternate_columns": [
            "Supplier", "Product", "Store",
            "Date", "QtyOnHand", "MinQty", "UnitCost"
        ],
        "extra_columns": ["UnitCost"],
    },
}


#################################################################################################################
#                                               EXTRACR ONE FILE
#################################################################################################################

def extract_supplier_file(file_path, supplier_type):
    """Read CSV and normalize supported supplier schemas."""

    cfg = SUPPLIERS[supplier_type.upper()]

    with Path(file_path).open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(
            handle,
            delimiter=cfg["delimiter"]
        )

        actual_columns = reader.fieldnames
        expected_columns = cfg["columns"]
        alternate_columns = cfg.get("alternate_columns")

        if actual_columns not in (expected_columns, alternate_columns):
            raise ValueError(
                f"Unexpected CSV columns in {Path(file_path).name}: "
                f"{actual_columns}"
            )

        rows = list(reader)

    for line, row in enumerate(rows, start=2):

        if None in row or any(
            row[col] is None for col in actual_columns
        ):
            raise ValueError(
                f"Malformed CSV row {line} in {Path(file_path).name}"
            )

        # Normalize GENERAL's new schema
        if supplier_type.upper() == "GENERAL":

            if "QtyOnHand" in row:
                row["Qty"] = row.pop("QtyOnHand")

            # Old files do not have UnitCost
            row.setdefault("UnitCost", None)

    return rows


#################################################################################################################
#                                               LOAD ONE FILE
#################################################################################################################

def load_supplier_file(rows, file_path, supplier_type):
    """Insert a complete file and its ingestion_files record in one transaction."""
    
    # get configuration
    supplier_type = supplier_type.upper()
    cfg = SUPPLIERS[supplier_type]
    filename = Path(file_path).name
    columns = (cfg["columns"]+ cfg.get("extra_columns", [])+ ["source_file", "source_row_number"])
    
    
    insert = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) "
        "ON CONFLICT (source_file, source_row_number) DO NOTHING"
    ).format(
        sql.Identifier("bronze_suppliers", cfg["table"]),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    
    # prepare values that come from extract func 
    values = [
        tuple(
            row.get(col)
            for col in cfg["columns"] + cfg.get("extra_columns", [])
        ) + (filename, line)
        for line, row in enumerate(rows, start=2)
    ]

    # Connect with warehouse
    with get_warehouse_connection() as conn, conn.cursor() as cur:
        
        # Prevent two simultaneous runs from ingesting the same supplier file.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                    (supplier_type, filename))
        
        # check if file name on ingest_files or not 
        cur.execute(
            "SELECT 1 FROM bronze_suppliers.ingestion_files "
            "WHERE supplier_type=%s AND source_file=%s",
            (supplier_type, filename),
        )
        
        # if true : file exists
        if cur.fetchone():
            return {"status": "SKIPPED", "rows_extracted": 0, "rows_loaded": 0}
        
        # if not : file not exists >> load data
        if values:
            cur.executemany(insert, values)
            inserted = cur.rowcount
        else:
            inserted = 0
        
        # save logs at ingestion files    
        cur.execute(
            "INSERT INTO bronze_suppliers.ingestion_files "
            "(supplier_type, source_file, row_count) VALUES (%s, %s, %s)",
            (supplier_type, filename, len(rows)),
        )
        
    return {"status": "SUCCESS", "rows_extracted": len(rows), "rows_loaded": inserted}



#################################################################################################################
#                                               Main Function
#################################################################################################################

def load_all_supplier_files(supplier_types=("FRESH", "GENERAL"), load_type="INCREMENTAL",start_date=None, end_date=None):
    
    """Discover missing files and record each supplier run in etl_control.load_runs.
    FULL/INCREMENTAL: ingest all unregistered files (including late arrivals).
    BACKFILL: ingest only unregistered files with filename dates in [start_date, end_date].
    Already registered files are always skipped; BACKFILL does not force reload.
    """
    
#############   VALIDATIONS of load type   #######################
    load_type = load_type.upper()
    
    if load_type not in {"FULL", "INCREMENTAL", "BACKFILL"}:
        raise ValueError("load_type must be FULL, INCREMENTAL, or BACKFILL")
    
    if load_type == "BACKFILL":
        if start_date is None or end_date is None:
            raise ValueError("BACKFILL requires start_date and end_date (YYYY-MM-DD)")
        start = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
        end = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
        if not isinstance(start, date) or not isinstance(end, date) or start > end:
            raise ValueError("Invalid BACKFILL date range")
    elif start_date is not None or end_date is not None:
        raise ValueError("start_date/end_date are only supported for BACKFILL")


############# Prepare summary and errors #######################
    summary = {"loaded_files": 0, 
               "skipped_files": 0, 
               "failed_files": 0,
               "rows_extracted": 0,
               "rows_loaded": 0
               }
    
    all_errors = []


#############  MAIN PROCESS   #######################

    for supplier_type in supplier_types:
        
        #############   VALIDATIONS of suppliers  #######################
        supplier_type = supplier_type.upper()
        
        if supplier_type not in SUPPLIERS:
            raise ValueError(f"Unknown supplier: {supplier_type}")
        
        cfg = SUPPLIERS[supplier_type]
        folder = SUPPLIER_DIR / cfg["folder"]
        
        if not folder.is_dir():
            raise FileNotFoundError(f"Supplier directory not found: {folder}")

        # Register RUNNING "etl logs" before reading files, so a failed run is visible.
        with get_warehouse_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_control.load_runs "
                "(source_name, load_type, status, rows_extracted, rows_loaded) "
                "VALUES (%s, %s, 'RUNNING', 0, 0) RETURNING run_id",
                (f"SUPPLIER_{supplier_type}", load_type),
            )
            run_id = cur.fetchone()[0]


        # initialize vars
        extracted = inserted = 0
        errors = []
        
        # 
        try:
            # get already loaded files name
            with get_warehouse_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT source_file FROM bronze_suppliers.ingestion_files "
                    "WHERE supplier_type=%s", (supplier_type,)
                )
                # loaded_names = all loaded files
                loaded_names = {record[0] for record in cur.fetchall()}

            # loop at files 
            for path in sorted(folder.glob(cfg["pattern"])):
                if load_type == "BACKFILL":
                    try:
                        file_date = datetime.strptime(
                            path.stem.removeprefix(cfg["prefix"]), cfg["date_format"]
                        ).date()
                    except ValueError:
                        errors.append(f"Invalid filename date: {path.name}")
                        summary["failed_files"] += 1
                        continue
                    if not start <= file_date <= end:
                        continue

                # if path already loaded
                if path.name in loaded_names:
                    summary["skipped_files"] += 1
                    continue
                
                # if path not loaded "read and load"
                try:
                    rows = extract_supplier_file(path, supplier_type)
                    result = load_supplier_file(rows, path, supplier_type)
                    
                    # save result
                    if result["status"] == "SKIPPED":
                        summary["skipped_files"] += 1
                    else:
                        summary["loaded_files"] += 1
                        extracted += result["rows_extracted"]
                        inserted += result["rows_loaded"]
                        logger.info("Loaded %s: %s rows", path.name, result["rows_loaded"])
                
                # if process failed        
                except Exception as exc:
                    errors.append(f"{path.name}: {exc}")
                    summary["failed_files"] += 1
                    logger.exception("Failed supplier file %s", path.name)
                    
        except Exception as exc:
            errors.append(f"Supplier discovery: {exc}")
            logger.exception("Failed supplier discovery: %s", supplier_type)
            
        # modify etl logs with final result    
        finally:
            # Persist run status even when extraction or loading fails.
            with get_warehouse_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE etl_control.load_runs SET finished_at=NOW(), status=%s, "
                    "rows_extracted=%s, rows_loaded=%s, error_message=%s WHERE run_id=%s",
                    ("FAILED" if errors else "SUCCESS", extracted, inserted,
                     "\n".join(errors)[:5000] if errors else None, run_id),
                )
            summary["rows_extracted"] += extracted
            summary["rows_loaded"] += inserted
            all_errors.extend(errors)

    logger.info("Supplier ingestion summary: %s", summary)
    if all_errors:
        raise RuntimeError(f"{len(all_errors)} supplier error(s); inspect etl_control.load_runs")
    return summary
