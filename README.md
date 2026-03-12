# NYC EMS Incident Analytics Pipeline

An end-to-end data engineering and analytics project built on **556,000+ real NYC Emergency Medical Services incidents** (May – Dec 2025). Covers data ingestion, transformation, and an interactive Streamlit dashboard with a RAG-ready chatbot scaffold.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | NYC Open Data Socrata API (`76xm-jjuj`) |
| Ingestion | Python, sodapy, pandas |
| Storage | DuckDB (embedded, file-based) |
| Transformation | dbt (dbt-duckdb) |
| Dashboard | Streamlit, Altair |
| Language | Python 3.12 |

---

## Architecture

```
NYC Socrata API
  → ingestion/pull_data.py    # Paginated fetch (50k rows/request), 3-retry logic
  → ingestion/data_sql.py     # Append-only load into DuckDB with run_id + ingestion_time
  → data/raw.duckdb           # Local embedded database (no server required)
  → dbt staging               # stg_incident: type casting, dedup, borough filter
  → dbt intermediate          # int_enrichment: temporal features, response category, seed joins
  → dbt marts                 # 8 analytical tables
  → streamlit_app.py          # Interactive dashboard with live DuckDB queries
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

**Chatbot tab:** Scaffolded UI with suggestion chips and `st.chat_input` — ready to wire up an LLM for natural-language queries over the EMS data.

---

## Key Engineering Decisions

- **Append-only ingestion** — raw data accumulates across runs; deduplication happens downstream in the staging layer via `ROW_NUMBER() OVER (PARTITION BY cad_incident_id)`
- **DuckDB as the single store** — no external database server; the `.duckdb` file is the entire data warehouse
- **dbt for all transformations** — SQL is version-controlled, testable, and documented; no ad-hoc pandas transforms in the pipeline
- **Live dashboard queries** — the dashboard bypasses pre-aggregated mart tables and queries `int_enrichment` directly so date and borough filters work without re-running dbt
- **Configurable date range** — ingestion window is controlled by `START_DATE` / `END_DATE` env vars; no code changes needed to re-ingest a different period

---

## Running the Project

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Set up environment**
```bash
cp .env.example .env
# Add your NYC Open Data API token: APP_TOKEN=...
```

**3. Run ingestion**
```bash
python ingestion/main.py
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
**Period:** May 1 – Dec 31, 2025
**Volume:** 556,490 incidents across 5 NYC boroughs
**Key fields:** incident datetime, borough, dispatch/response times, call type (initial + final), incident disposition, special event indicator
