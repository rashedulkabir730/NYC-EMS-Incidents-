"""
main.py — entry point for the EMS ingestion pipeline.

Old flow:  pull_data() → load_data_duckdb()   (manual pandas + DuckDB)
New flow:  dlt_pipeline.run() → verify_load() (dlt handles the load)

Usage:
    python -m ingestion.main            # normal incremental run
    python -m ingestion.main --reset    # wipe cursor state + full reload
"""

import sys
import logging
from ingestion.dlt_pipeline import run as dlt_run
from ingestion.data_sql import verify_load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    reset = "--reset" in sys.argv

    logging.info("Starting EMS ingestion pipeline (reset=%s)", reset)

    # dlt fetches from Socrata, infers schema, and loads into DuckDB.
    # On the first run it pulls everything in YEAR_START → YEAR_END.
    # On subsequent runs it only fetches rows newer than the last cursor.
    dlt_run(reset=reset)

    # Confirm the load by querying the dlt-managed table directly.
    stats = verify_load()
    logging.info(
        "Pipeline complete — %s rows | %s → %s",
        stats["row_count"],
        stats["min_date"],
        stats["max_date"],
    )


if __name__ == "__main__":
    main()
