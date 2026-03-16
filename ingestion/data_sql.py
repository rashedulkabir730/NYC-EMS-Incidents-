"""
data_sql.py — post-load verification utilities for the dlt pipeline.

dlt now handles all CREATE TABLE / INSERT logic, so this module no longer
manually loads data into DuckDB.  Instead it provides helper functions that
you can call after pipeline.run() to confirm the load succeeded and inspect
what landed in the database.

The dlt pipeline writes to:
    data/raw.duckdb  →  schema: raw_dlt  →  table: raw_incidents
"""

import duckdb
import logging

# Path to the shared DuckDB file — same one dlt writes to and dbt reads from.
DB_PATH = "data/raw.duckdb"

# Fully-qualified table name that dlt creates.
# If you changed dataset_name in dlt_pipeline.py, update this too.
DLT_TABLE = "raw_dlt.raw_incidents"


def verify_load() -> dict:
    """
    Run a quick sanity check on the dlt-managed table after a pipeline run.

    Returns a dict with:
        row_count   — total rows currently in the table
        min_date    — earliest incident_datetime in the table
        max_date    — latest  incident_datetime in the table

    Call this right after dlt_pipeline.run() to confirm data landed correctly.

    Example
    -------
    from ingestion.data_sql import verify_load
    stats = verify_load()
    print(stats)
    # {'row_count': 312450, 'min_date': '2025-01-01 ...', 'max_date': '2025-12-31 ...'}
    """
    con = duckdb.connect(DB_PATH, read_only=True)

    try:
        # Check the table actually exists before querying it.
        # dlt creates it on the first successful run; if the pipeline has never
        # run (or was reset and re-run), this guard prevents a confusing error.
        table_exists = con.execute(f"""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'raw_dlt'
              AND table_name   = 'raw_incidents'
        """).fetchone()

        if table_exists is None:
            logging.warning(
                "Table %s does not exist yet — has the dlt pipeline run?",
                DLT_TABLE,
            )
            return {"row_count": 0, "min_date": None, "max_date": None}

        row = con.execute(f"""
            SELECT
                COUNT(*)                        AS row_count,
                MIN(incident_datetime)::VARCHAR AS min_date,
                MAX(incident_datetime)::VARCHAR AS max_date
            FROM {DLT_TABLE}
        """).fetchone()

    finally:
        con.close()

    stats = {
        "row_count": row[0],
        "min_date":  row[1],
        "max_date":  row[2],
    }

    logging.info(
        "Load verified — %s rows | %s → %s",
        stats["row_count"],
        stats["min_date"],
        stats["max_date"],
    )

    return stats
