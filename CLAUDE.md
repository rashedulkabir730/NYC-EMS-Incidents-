# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Run ingestion pipeline:**
```bash
python ingestion/main.py
```

**Run dbt transformations:**
```bash
cd dbt && dbt run
```

**Run a single dbt model:**
```bash
cd dbt && dbt run --select <model_name>
# e.g. dbt run --select stg_incident
```

**Run dbt tests:**
```bash
cd dbt && dbt test
```

**Seed reference data:**
```bash
cd dbt && dbt seed
```

**Generate and serve dbt docs:**
```bash
cd dbt && dbt docs generate && dbt docs serve
# Opens at http://localhost:8080
```

**Install dependencies:**
```bash
pip install -r requirements.txt
```

## Architecture

This is a local EMS incident analytics pipeline for NYC data. There is no orchestration layer — the pipeline is run manually in two steps: ingest, then transform.

### Data Flow

```
NYC Socrata API (dataset: 76xm-jjuj)
  → ingestion/pull_data.py   # paginated fetch (50k records/request), 3-retry logic
  → ingestion/data_sql.py    # appends to raw_api_data table in DuckDB, adds run_id + ingestion_time
  → data/raw.duckdb          # local embedded database, no server required
  → dbt staging              # stg_incident: type casting, dedup on cad_incident_id, filter UNKNOWN boroughs
  → dbt intermediate         # int_enrichment: temporal features + LEFT JOINs with seed lookup tables
  → dbt marts                # 8 analytical tables (response times, incident counts, MoM growth, etc.)
```

### Key Design Decisions

- **DuckDB** is used as the only database — no external DB server. The file is at `data/raw.duckdb`.
- **dbt profile** (`~/.dbt/profiles.yml`) points to the DuckDB file. The profile name is `ems_analytics`.
- All dbt models are materialized as **tables** (not views).
- The ingestion script **appends** data; deduplication happens in the staging layer using `ROW_NUMBER() OVER (PARTITION BY cad_incident_id)`.
- The NYC API token is loaded from `.env` as `APP_TOKEN`.
- Data date range is controlled by `START_DATE` and `END_DATE` env vars (defaults: May 1 – Dec 31, 2025). Set these in `.env` to change the ingestion window without modifying code.

### dbt Layer Summary

| Layer | Model | Purpose |
|---|---|---|
| Staging | `stg_incident` | Cast types, trim/upper strings, deduplicate, filter invalid boroughs |
| Intermediate | `int_enrichment` | Add day/month/year/hour columns, response_category (Fast/Moderate/Slow), join call type and disposition seed tables |
| Marts | `marts_avg_response_time` | Avg dispatch + response time by borough (valid records only) |
| Marts | `marts_incident_dow` | Incidents by day of week and borough |
| Marts | `marts_incident_growth_MoM` | MoM incident counts with LAG-based % change |
| Marts | `marts_rank_incidents` | Borough ranking by total incidents |
| Marts | `marts_response_cat_borough` | Fast/Moderate/Slow breakdown by borough |
| Marts | `marts_special_events` | Incidents where special_event_indicator = 'Y' |
| Marts | `marts_total_incidents_by_final_call_type` | Counts by final call type |
| Marts | `marts_total_incidents_by_initial_call_type_desc` | Counts by initial call type description |

### Schema / Documentation YML Files

Each dbt layer has a companion yml file for column descriptions (used for dbt docs and RAG):
- `dbt/models/staging/_stg_incident.yml` — tests (unique, not_null, accepted_values) + staging columns
- `dbt/models/intermediate/_int_enrichment.yml` — all enriched columns with full descriptions
- `dbt/models/marts/marts.yml` — all mart models and their output columns

Always update the relevant yml when adding or renaming columns or models.

### Seeds (Reference Data)

Two CSV seed files in `dbt/seeds/` map codes to human-readable descriptions:
- **Call Type Descriptions** — maps call codes (e.g. `ARREST`, `ASTHMA`) to descriptions
- **Incident Dispositions** — maps disposition codes (e.g. `82`, `83`) to outcomes

These are joined in `int_enrichment` to enrich incidents with readable labels.
