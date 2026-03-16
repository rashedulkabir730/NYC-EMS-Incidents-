# NYC EMS Incident Analytics Pipeline

An end-to-end data engineering and AI analytics project built on **1,079,491 real NYC Emergency Medical Services incidents** (Jan – Aug 2025). Covers data ingestion, transformation, an interactive Streamlit dashboard, and a fully functional RAG-powered chatbot that answers natural-language questions about the data using live SQL and the Claude API.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | NYC Open Data Socrata API (`76xm-jjuj`) |
| Ingestion | dlt (data load tool), sodapy |
| Storage | DuckDB (embedded, file-based) |
| Transformation | dbt (dbt-duckdb) |
| Dashboard | Streamlit, Altair |
| AI / RAG | Claude API (claude-sonnet-4-6), ChromaDB |
| Language | Python 3.12 |

---

## Architecture

```
NYC Socrata API
  → ingestion/dlt_pipeline.py   # dlt resource: paginated fetch, incremental cursor
  → ingestion/data_sql.py       # Post-load verification (row count, date range check)
  → data/raw.duckdb             # Local embedded database — schema: raw_dlt
  → dbt staging                 # stg_incident: type casting, dedup, borough filter
  → dbt intermediate            # int_enrichment: temporal features, response category, seed joins
  → dbt marts                   # 8 analytical tables
  → streamlit_app.py            # Interactive dashboard with live DuckDB queries
  → rag.py                      # RAG module: ChromaDB + Claude tool-use for natural-language Q&A
```

### dbt Layers

| Layer | Model | Description |
|---|---|---|
| Staging | `stg_incident` | Casts types, trims strings, deduplicates on `cad_incident_id`, filters invalid boroughs |
| Intermediate | `int_enrichment` | Adds day/month/year/hour columns, response speed category (Fast/Moderate/Slow), joins call type and disposition lookup tables |
| Marts | `marts_avg_response_time` | Average dispatch and on-scene response time by borough |
| Marts | `marts_incident_dow` | Incident counts by day of week and borough |
| Marts | `marts_incident_growth_MoM` | Month-over-month volume with LAG-based % change |
| Marts | `marts_rank_incidents` | Borough ranking by total incident volume |
| Marts | `marts_response_cat_borough` | Fast/Moderate/Slow breakdown by borough |
| Marts | `marts_special_events` | Incidents during planned special events by borough |
| Marts | `marts_total_incidents_by_initial_call_type_desc` | Counts by initial call classification |
| Marts | `marts_total_incidents_by_final_call_type` | Counts by final (resolved) call classification |

---

## Dashboard

Built with **Streamlit + Altair**. Queries `int_enrichment` directly for live, filter-aware results — all charts respond instantly to sidebar filters.

**Filters:**
- Date range picker with one-click reset to full data span
- Borough multi-select

**Charts & KPIs:**
- KPI cards: total incidents, avg response time, avg dispatch time, special event count
- Incidents by borough (horizontal bar)
- Avg dispatch vs. on-scene response time by borough (grouped bar)
- Response speed breakdown — Fast / Moderate / Slow (100% stacked bar)
- Month-over-month incident volume with directional coloring (bar)
- Incidents by day of week (heatmap)
- Special event incidents by borough (bar)
- Top 15 initial and final call types — chart + sortable table toggle

**Color system:** Each borough has a fixed identity color across all charts. Response speed uses semantic green/orange/red. All queries are cached per filter combination (`@st.cache_data`).

**EMS Assistant (chatbot tab):** Fully functional RAG-powered chatbot. Ask questions in plain English — the assistant retrieves relevant schema context from ChromaDB, then uses Claude's tool-use API to run live SQL against DuckDB and return data-backed answers. Supports full multi-turn conversation with session history.

---

## Key Engineering Decisions

- **dlt for ingestion** — replaces manual pandas/sodapy loading. dlt handles schema inference, state management, and incremental loading so re-runs only fetch rows newer than the last cursor position. The first run pulls all of 2025; subsequent runs are fast and cheap.
- **Incremental cursor** — dlt tracks `incident_datetime` between runs in `.dlt/` state. Delete that folder (or run `--reset`) to trigger a full reload.
- **Append-only write disposition** — raw data accumulates across runs; deduplication happens downstream in the staging layer via `ROW_NUMBER() OVER (PARTITION BY cad_incident_id)`
- **DuckDB as the single store** — no external database server; the `.duckdb` file is the entire data warehouse. dlt writes to the `raw_dlt` schema; dbt transforms into the `test` schema.
- **dbt for all transformations** — SQL is version-controlled, testable, and documented; no ad-hoc pandas transforms in the pipeline
- **Live dashboard queries** — the dashboard bypasses pre-aggregated mart tables and queries `int_enrichment` directly so date and borough filters work without re-running dbt
- **RAG + tool-use chatbot** — `rag.py` loads dbt YAML schema docs into ChromaDB at startup; the assistant uses vector retrieval to find relevant schema context, then calls the Claude API with a `run_query` tool that executes live DuckDB SQL to answer factual questions

---

## EMS Assistant (RAG Chatbot)

The chatbot tab in the dashboard lets you query the data in plain English. It uses a two-stage pipeline:

1. **Retrieval** — dbt model and column descriptions from all YAML files are embedded in ChromaDB. The user's question is used to retrieve the most relevant schema context.
2. **Generation** — Claude receives the schema context and can call a `run_query` tool to execute SQL against the live DuckDB database. It then synthesises a natural-language answer from the results.

Example questions it can answer:
- *"Which borough has the slowest average response time?"*
- *"How many incidents happened during special events in Brooklyn?"*
- *"What's the most common emergency call type on weekends?"*
- *"Show me month-over-month incident growth for the Bronx"*

**Required:** Set `ANTHROPIC_API_KEY` in `.env` (get a key at [console.anthropic.com](https://console.anthropic.com)).

---

## Running the Project

**1. Install dependencies**
```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**2. Set up environment**
```bash
cp .env.example .env
# Add your NYC Open Data API token: APP_TOKEN=...
# Add your Anthropic API key for the chatbot: ANTHROPIC_API_KEY=...
```

**3. Run ingestion**
```bash
# First run — pulls all of 2025 (Jan–Dec, ~1M+ rows)
python -m ingestion.main

# Subsequent runs — incremental, only fetches new rows
python -m ingestion.main

# Full reload from scratch
python -m ingestion.main --reset
```

**4. Run dbt transformations**
```bash
cd dbt && dbt run
cd dbt && dbt test
```

**5. Launch dashboard**
```bash
streamlit run streamlit_app.py
# Opens at http://localhost:8501
```

---

## Dataset

**Source:** [NYC Open Data — EMS Incident Dispatch Data](https://data.cityofnewyork.us/Public-Safety/EMS-Incident-Dispatch-Data/76xm-jjuj)
**Period:** Jan 1 – Aug 31, 2025 (full year pulled; upstream data published through Aug)
**Volume:** 1,079,491 incidents across 5 NYC boroughs
**Key fields:** incident datetime, borough, dispatch/response times, call type (initial + final), incident disposition, special event indicator
