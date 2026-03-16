# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Run ingestion pipeline:**
```bash
# Incremental run (only fetches rows newer than the last cursor)
python -m ingestion.main

# Full reload from scratch (wipes dlt state, re-fetches all of 2025)
python -m ingestion.main --reset
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
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Run the Streamlit dashboard:**
```bash
streamlit run streamlit_app.py
# Opens at http://localhost:8501
```

## Architecture

This is a local EMS incident analytics pipeline for NYC data. There is no orchestration layer — the pipeline is run manually in two steps: ingest, then transform.

### Data Flow

```
NYC Socrata API (dataset: 76xm-jjuj)
  → ingestion/dlt_pipeline.py  # dlt resource: paginated fetch (50k rows/page), incremental cursor on incident_datetime
  → ingestion/data_sql.py      # post-load verification: row count + date range check against raw_dlt.raw_incidents
  → data/raw.duckdb            # local embedded database, schema: raw_dlt, table: raw_incidents
  → dbt staging                # stg_incident: type casting, dedup on cad_incident_id, filter UNKNOWN boroughs
  → dbt intermediate           # int_enrichment: temporal features + LEFT JOINs with seed lookup tables
  → dbt marts                  # 8 analytical tables (response times, incident counts, MoM growth, etc.)
```

### Key Design Decisions

- **dlt for ingestion** — dlt handles schema inference, state management, and incremental loading. On the first run it pulls all of 2025 (Jan–Dec). On re-runs it only fetches rows with `incident_datetime > last cursor value`. Cursor state is stored in `.dlt/pipelines/ems_incidents/`.
- **DuckDB** is used as the only database — no external DB server. The file is at `data/raw.duckdb`.
  - dlt writes to the `raw_dlt` schema (table: `raw_incidents`)
  - dbt transforms into the `test` schema
- **dbt profile** (`~/.dbt/profiles.yml`) points to the DuckDB file. The profile name is `ems_analytics`.
- All dbt models are materialized as **tables** (not views).
- The dlt resource uses `write_disposition="append"`; deduplication happens in the staging layer using `ROW_NUMBER() OVER (PARTITION BY cad_incident_id)`.
- The NYC API token is loaded from `.env` as `APP_TOKEN`.
- The full ingestion window is `YEAR_START` / `YEAR_END` constants in `dlt_pipeline.py` (defaults: Jan 1 – Dec 31, 2025).

### Ingestion Module Summary

| File | Role |
|---|---|
| `ingestion/dlt_pipeline.py` | dlt `@dlt.resource` generator + `run()` function. Paginates Socrata API, yields rows to dlt, manages incremental cursor. Entry point for the pipeline. |
| `ingestion/data_sql.py` | Post-load utility. `verify_load()` queries `raw_dlt.raw_incidents` and returns row count + min/max date. Called after `dlt_run()` in `main.py`. |
| `ingestion/main.py` | Orchestrates: calls `dlt_run(reset=...)` then `verify_load()`. Supports `--reset` flag. |

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

### dbt Source

The dbt staging model reads from the dlt-managed table:
- **Source name:** `raw` (defined in `dbt/models/staging/sources.yml`)
- **Schema:** `raw_dlt`
- **Table:** `raw_incidents`

Always update `sources.yml` if the dlt `dataset_name` or resource `name` changes in `dlt_pipeline.py`.

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

---

## Streamlit Dashboard

**Entry point:** `streamlit_app.py`
**Theme:** `.streamlit/config.toml` — red primary, dark navy sidebar, Inter font

The dashboard queries `test.int_enrichment` directly (bypassing pre-aggregated mart tables) so that date and borough filters apply to every chart without re-running dbt.

### Sidebar filters

- **Date range** — `st.date_input` with min/max derived from actual data bounds; **Reset dates** button snaps back to full range
- **Borough** — multiselect; defaults to all 5 boroughs

### Charts

| Chart | Source query |
|---|---|
| KPI cards (total incidents, avg response, avg dispatch, special events) | Direct aggregates from `int_enrichment` |
| Incidents by borough (bar) | `COUNT(*) GROUP BY borough` |
| Avg response time by borough — Dispatch vs On-scene (grouped bar) | `AVG(dispatch/incident_response_seconds_qy)` |
| Response speed breakdown — Fast/Moderate/Slow (100% stacked bar) | `COUNT(*) GROUP BY borough, response_category` |
| Month-over-month incident volume (bar, directional color) | `COUNT(*) GROUP BY EXTRACT(YEAR/MONTH)` + LAG |
| Incidents by day of week (heatmap) | `COUNT(*) GROUP BY borough, day_of_week_of_incident` |
| Special event incidents by borough (bar) | `WHERE special_event_indicator = 'Y'` |
| Top 15 initial call types — chart + table toggle | `COUNT(*) GROUP BY initial_call_type_desc` |
| Top 15 final call types — chart + table toggle | `COUNT(*) GROUP BY final_call_type_desc` |

### Color system

- Each borough has a **fixed color** across all charts (Bronx=red, Brooklyn=blue, Manhattan=green, Queens=purple, Staten Island=orange)
- Response speed: semantic green / orange / red
- MoM direction: red = volume grew, green = volume declined
- Single-series bars: accent blue
- All colors defined as constants at the top of `streamlit_app.py`

### Caching

All query functions use `@st.cache_data(ttl=3600)` keyed on `(start_date, end_date, boroughs_tuple)` — repeated filter selections are served instantly from cache.

### Chatbot tab (EMS Assistant)

Fully wired to `rag.py`. On each user message:
1. `retrieve_context(question)` queries ChromaDB for the top-3 relevant dbt schema docs
2. `ask_ems(question, history)` calls Claude with a system prompt + schema context + a `run_query` tool
3. Claude executes SQL via the tool, then returns a natural-language answer
4. The answer is displayed with `st.markdown`; session history is maintained in `st.session_state.messages`

The history passed to `ask_ems` is all messages before the current question. Tool-use content blocks are serialised with `.model_dump()` before being appended to the messages array (fixes Pydantic v2 `by_alias` serialisation bug).

### dbt schema note

dbt materializes all models into the `test` schema (configured in `~/.dbt/profiles.yml`). All dashboard queries therefore use `test.<table_name>` explicitly.

---

## RAG Module (`rag.py`)

**Entry point:** `rag.py`
**Dependencies:** `chromadb`, `anthropic`, `pyyaml`, `duckdb`, `python-dotenv`

### Initialisation (runs once at import time)

- Walks all YAML files under `dbt/models/` and builds one text document per dbt model (name + description + all column descriptions)
- Loads documents into an in-memory ChromaDB collection (`ems_metadata`)
- Initialises `anthropic.Anthropic()` client (reads `ANTHROPIC_API_KEY` from `.env`)

### Public API

- `retrieve_context(question: str) -> str` — queries ChromaDB for top-3 relevant schema docs, returns them joined as a string
- `ask_ems(question: str, history: list[dict]) -> str` — full pipeline:
  1. Retrieve schema context
  2. Call Claude (`claude-sonnet-4-6`) with system prompt + context + `run_query` tool
  3. If Claude calls the tool, execute the SQL against `data/raw.duckdb` (read-only) and feed results back
  4. Loop until Claude returns a final text answer

### Tool definition

The `run_query` tool takes a single `sql` string and runs it against `test.int_enrichment`. Results are returned as a plain-text table (max 50 rows). Query errors are caught and returned as strings so Claude can self-correct.

### Known quirks

- ChromaDB client is in-memory — documents reload every time the Python process starts (fast enough, ~8 documents)
- Pydantic v2 `by_alias` bug: tool-use `response.content` blocks must be serialised with `.model_dump()` before appending to the messages list
- `ANTHROPIC_API_KEY` must be set in `.env`; the module raises at import time if the key is missing
