

import sys
import dlt
from sodapy import Socrata
import os
from dotenv import load_dotenv

load_dotenv()

SOCRATA_DOMAIN = "data.cityofnewyork.us"
DATASET_ID     = "76xm-jjuj"
APP_TOKEN      = os.getenv("APP_TOKEN")  # same .env key you already use


YEAR_START = "2025-01-01T00:00:00.000"
YEAR_END   = "2025-12-31T23:59:59.999"

PAGE_SIZE = 50_000  


# ── Resource ──────────────────────────────────────────────────────────────────

@dlt.resource(
    name="raw_incidents",           
    write_disposition="append",           
    primary_key="cad_incident_id", 
)
def ems_incidents(
    # ── Incremental cursor ───────────────────────────────────────────────────
    # dlt.sources.incremental wraps a column so that on re-runs dlt
    # automatically filters WHERE incident_datetime > <last saved value>.
    #
    # First run  → uses initial_value  (pulls all of 2025)
    # Second run → uses the max incident_datetime from the first run
    #              (pulls only new rows since then)
    #
    # The saved cursor lives in .dlt/pipelines/ems_incidents/state.json.
    # Delete that file (or run with --reset) to start over from YEAR_START.
    incident_datetime=dlt.sources.incremental(
        "incident_datetime",       # column name returned by the API
        initial_value=YEAR_START,
        end_value=YEAR_END,        # upper bound — dlt won't advance past this
    ),
):
    """
    Generator that pages through the Socrata API and yields one dict per row.

    dlt calls this function once per pipeline.run(). The `incident_datetime`
    argument is injected automatically by dlt with the current cursor window.
    Simply yield rows — dlt handles batching, type inference, and the load.
    """

    if not APP_TOKEN:
        raise EnvironmentError(
            "APP_TOKEN is not set. Add it to your .env file."
        )

    client = Socrata(SOCRATA_DOMAIN, app_token=APP_TOKEN, timeout=30)

    # dlt exposes the live cursor bounds through the incremental object.
    # On run 1: start=YEAR_START, end=YEAR_END
    # On run 2: start=<max datetime from run 1>, end=YEAR_END
    start = incident_datetime.last_value
    end   = incident_datetime.end_value

    where_clause = f"incident_datetime between '{start}' and '{end}'"

    print(f"\n→ Fetching records where {where_clause}")

    offset = 0
    total  = 0

    while True:
        # ── Fetch one page ───────────────────────────────────────────────────
        try:
            batch = client.get(
                DATASET_ID,
                where=where_clause,
                offset=offset,
                limit=PAGE_SIZE,
                # IMPORTANT: always order by the cursor column so the max
                # value at the end of each page advances monotonically.
                # Without this the cursor could jump backwards on retry.
                order="incident_datetime ASC",
            )
        except Exception as exc:
            # Re-raise so dlt marks the run as FAILED and does NOT advance
            # the cursor. On the next run you'll retry from the same offset.
            raise RuntimeError(
                f"Socrata API error at offset {offset}: {exc}"
            ) from exc

        if not batch:
            break  # Socrata returned an empty page → we've read everything

        # yield rows one at a time — dlt accumulates them internally and
        # writes to DuckDB in configurable chunks (default ~10k rows).
        yield from batch

        total  += len(batch)
        offset += PAGE_SIZE
        print(f"  … {total:,} rows fetched so far")

    print(f"\n✓ Done — {total:,} total rows yielded to dlt")


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run(reset: bool = False):
    """
    Build and execute the dlt pipeline.

    Parameters
    ----------
    reset : bool
        If True, wipe the saved cursor state before running so the full
        2025 window is re-fetched from scratch. Useful when you want a
        clean reload (e.g. after changing schema or fixing a bad run).
    """
    pipeline = dlt.pipeline(
        pipeline_name="ems_incidents",      # also the folder under .dlt/
        destination=dlt.destinations.duckdb(
            credentials="data/raw.duckdb"   # reuse your existing DuckDB file
        ),
        dataset_name="raw_dlt",             # schema name inside DuckDB
                                            # table lands at raw_dlt.raw_incidents
    )

    if reset:
        # Drop the saved cursor so the next run starts from YEAR_START again.
        # NOTE: this does NOT truncate the DuckDB table — do that manually if
        # you also want to clear existing rows:
        #   duckdb.connect("data/raw.duckdb").execute("DELETE FROM raw_dlt.raw_incidents")
        pipeline.drop()
        print("✓ Pipeline state reset — will re-fetch full 2025 window")

    # pipeline.run() orchestrates the resource generator → DuckDB load.
    # It returns a LoadInfo object with row counts, timing, and any errors.
    load_info = pipeline.run(ems_incidents())

    # Print a human-readable summary (packages loaded, rows written, etc.)
    print("\n── Load summary ─────────────────────────────────────────────────")
    print(load_info)

    # ── Optional: quick sanity check ─────────────────────────────────────────
    # Uncomment to verify the row count right after the load:
    #
    # with pipeline.sql_client() as client:
    #     with client.execute_query(
    #         "SELECT COUNT(*) AS n, MIN(incident_datetime), MAX(incident_datetime)"
    #         " FROM raw_incidents"
    #     ) as cur:
    #         print("Table stats:", cur.fetchone())


if __name__ == "__main__":
    # Allow `python -m ingestion.dlt_pipeline --reset` from the project root
    reset_flag = "--reset" in sys.argv
    run(reset=reset_flag)
