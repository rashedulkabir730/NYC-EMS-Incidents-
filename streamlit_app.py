"""
NYC EMS Incidents Dashboard

Reads directly from test.int_enrichment in data/raw.duckdb with date + borough filters.
Run dbt first: cd dbt && dbt run
Then launch: streamlit run streamlit_app.py
"""

import datetime
import duckdb
import pandas as pd
import altair as alt
import streamlit as st

# RAG module — initialises ChromaDB once at import time
try:
    from rag import ask_ems
    RAG_READY = True
except Exception as _rag_err:
    RAG_READY = False
    _RAG_ERROR = str(_rag_err)

# =============================================================================
# Page config — must be first Streamlit call
# =============================================================================

st.set_page_config(
    page_title="NYC EMS Incidents",
    page_icon=":material/emergency:",
    layout="wide",
)

DB_PATH = "data/raw.duckdb"
CHART_HEIGHT = 300
DOW_LABELS = {
    "0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed",
    "4": "Thu", "5": "Fri", "6": "Sat",
}
MONTH_LABELS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# =============================================================================
# Color palette
# =============================================================================

BOROUGH_DOMAIN = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "RICHMOND / STATEN ISLAND"]
BOROUGH_RANGE  = ["#E74C3C", "#3498DB", "#2ECC71", "#9B59B6", "#E67E22"]

RESPONSE_DOMAIN = ["Fast", "Moderate", "Slow"]
RESPONSE_RANGE  = ["#2ECC71", "#E67E22", "#E74C3C"]

DUAL_DOMAIN = ["Dispatch", "On-scene"]
DUAL_RANGE  = ["#3498DB", "#E74C3C"]

MOM_POS = "#E74C3C"
MOM_NEG = "#2ECC71"
ACCENT  = "#3498DB"

# =============================================================================
# Query helpers
# =============================================================================

def _where(start: datetime.date, end: datetime.date, boroughs: tuple) -> str:
    """Build a WHERE clause for the given filters."""
    end_exclusive = end + datetime.timedelta(days=1)
    borough_list = ", ".join(f"'{b}'" for b in boroughs)
    return (
        f"incident_datetime >= '{start}' "
        f"AND incident_datetime < '{end_exclusive}' "
        f"AND borough IN ({borough_list})"
    )


@st.cache_data(ttl=3600)
def load_date_bounds() -> tuple[datetime.date, datetime.date]:
    con = duckdb.connect(DB_PATH, read_only=True)
    row = con.execute(
        "SELECT MIN(incident_datetime)::DATE, MAX(incident_datetime)::DATE "
        "FROM test.int_enrichment"
    ).fetchone()
    con.close()
    return row[0], row[1]


@st.cache_data(ttl=3600)
def load_boroughs() -> list[str]:
    con = duckdb.connect(DB_PATH, read_only=True)
    result = con.execute(
        "SELECT DISTINCT borough FROM test.int_enrichment ORDER BY borough"
    ).df()["borough"].tolist()
    con.close()
    return result


@st.cache_data(ttl=3600)
def load_kpis(start: datetime.date, end: datetime.date, boroughs: tuple) -> dict:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    total   = con.execute(f"SELECT COUNT(*) FROM test.int_enrichment WHERE {w}").fetchone()[0]
    avg_resp = con.execute(
        f"SELECT ROUND(AVG(incident_response_seconds_qy), 0) FROM test.int_enrichment "
        f"WHERE {w} AND valid_incident_rspns_time_indc = 'Y'"
    ).fetchone()[0]
    avg_disp = con.execute(
        f"SELECT ROUND(AVG(dispatch_response_seconds_qy), 0) FROM test.int_enrichment "
        f"WHERE {w} AND valid_dispatch_rspns_time_indc = 'Y'"
    ).fetchone()[0]
    special = con.execute(
        f"SELECT COUNT(*) FROM test.int_enrichment WHERE {w} AND special_event_indicator = 'Y'"
    ).fetchone()[0]
    con.close()
    return {"total": total, "avg_resp": avg_resp, "avg_disp": avg_disp, "special": special}


@st.cache_data(ttl=3600)
def load_borough_rank(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        WITH counts AS (
            SELECT borough, COUNT(*) AS total_incidents
            FROM test.int_enrichment WHERE {w}
            GROUP BY borough
        )
        SELECT borough, total_incidents,
               RANK() OVER (ORDER BY total_incidents DESC) AS rank
        FROM counts
    """).df()
    con.close()
    return df


@st.cache_data(ttl=3600)
def load_avg_response(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT borough,
               ROUND(AVG(dispatch_response_seconds_qy), 2)  AS avg_dispatch_response_seconds,
               ROUND(AVG(incident_response_seconds_qy), 2)  AS avg_incident_response_seconds
        FROM test.int_enrichment
        WHERE {w}
          AND valid_dispatch_rspns_time_indc = 'Y'
          AND valid_incident_rspns_time_indc = 'Y'
        GROUP BY borough
        ORDER BY avg_dispatch_response_seconds
    """).df()
    con.close()
    return df


@st.cache_data(ttl=3600)
def load_response_cat(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT borough, response_category, COUNT(*) AS total_incidents
        FROM test.int_enrichment WHERE {w}
        GROUP BY borough, response_category
    """).df()
    con.close()
    return df


@st.cache_data(ttl=3600)
def load_mom(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        WITH monthly AS (
            SELECT EXTRACT(YEAR FROM incident_datetime)  AS yr,
                   EXTRACT(MONTH FROM incident_datetime) AS month_of_incident,
                   COUNT(cad_incident_id) AS total_incidents
            FROM test.int_enrichment WHERE {w}
            GROUP BY EXTRACT(YEAR FROM incident_datetime), EXTRACT(MONTH FROM incident_datetime)
        )
        SELECT month_of_incident, total_incidents,
               total_incidents - LAG(total_incidents) OVER (ORDER BY yr, month_of_incident) AS mom_change,
               (total_incidents - LAG(total_incidents) OVER (ORDER BY yr, month_of_incident))
               / NULLIF(LAG(total_incidents) OVER (ORDER BY yr, month_of_incident), 0) AS mom_pct_change
        FROM monthly
        ORDER BY yr, month_of_incident
    """).df()
    con.close()
    return df


@st.cache_data(ttl=3600)
def load_dow(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT borough,
               TRY_CAST(day_of_week_of_incident AS VARCHAR) AS day_of_week_of_incident,
               COUNT(*) AS total_incidents
        FROM test.int_enrichment WHERE {w}
        GROUP BY borough, day_of_week_of_incident
    """).df()
    con.close()
    return df


@st.cache_data(ttl=3600)
def load_special_events(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT borough, COUNT(*) AS total_incidents_during_special_events
        FROM test.int_enrichment
        WHERE {w} AND special_event_indicator = 'Y'
        GROUP BY borough
        ORDER BY total_incidents_during_special_events DESC
    """).df()
    con.close()
    return df


@st.cache_data(ttl=3600)
def load_initial_call_types(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT initial_call_type_desc, COUNT(*) AS total_incidents
        FROM test.int_enrichment WHERE {w}
        GROUP BY initial_call_type_desc
        ORDER BY total_incidents DESC
    """).df()
    con.close()
    return df


@st.cache_data(ttl=3600)
def load_final_call_types(start: datetime.date, end: datetime.date, boroughs: tuple) -> pd.DataFrame:
    w = _where(start, end, boroughs)
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT final_call_type_desc, COUNT(*) AS total_incidents
        FROM test.int_enrichment WHERE {w}
        GROUP BY final_call_type_desc
        ORDER BY total_incidents DESC
    """).df()
    con.close()
    return df


# =============================================================================
# Sidebar — filters
# =============================================================================

with st.sidebar:
    st.markdown("## :material/emergency: NYC EMS")
    st.caption("New York City Emergency Medical Services — incident analytics dashboard.")
    st.write("")

    try:
        min_date, max_date = load_date_bounds()
        all_boroughs = load_boroughs()

        if st.button(":material/restart_alt: Reset dates", use_container_width=True):
            st.session_state["date_range"] = (min_date, max_date)

        date_range = st.date_input(
            "Date range",
            value=st.session_state.get("date_range", (min_date, max_date)),
            min_value=min_date,
            max_value=max_date,
            key="date_range",
        )
        # Handle partial selection (user picked only one date)
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start_date, end_date = date_range[0], date_range[1]
        else:
            start_date = end_date = date_range[0] if date_range else min_date

        selected_boroughs = st.multiselect(
            "Borough",
            options=all_boroughs,
            default=all_boroughs,
            placeholder="Select boroughs…",
        )
        if not selected_boroughs:
            selected_boroughs = all_boroughs

        filters_ok = True

    except Exception:
        filters_ok = False
        st.warning("Could not connect to database. Run `dbt run` first.")

    st.write("")
    st.write("")
    st.caption("Dataset: NYC Open Data · 76xm-jjuj")

# =============================================================================
# Tabs
# =============================================================================

tab_dashboard, tab_chatbot = st.tabs([
    ":material/dashboard: Dashboard",
    ":material/chat: EMS Assistant",
])

# =============================================================================
# Dashboard tab
# =============================================================================

with tab_dashboard:

    hdr_left, hdr_right = st.columns([8, 1])
    with hdr_left:
        st.markdown("# :material/emergency: NYC EMS Incident Analytics")
    with hdr_right:
        if st.button(":material/restart_alt: Reset", type="secondary"):
            st.cache_data.clear()
            st.rerun()

    if not filters_ok:
        st.error("Could not load data. Run `cd dbt && dbt run` first.", icon=":material/error:")
        st.stop()

    boroughs_tuple = tuple(sorted(selected_boroughs))

    try:
        # ── KPI row ───────────────────────────────────────────────────────────
        kpis = load_kpis(start_date, end_date, boroughs_tuple)
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            with st.container(border=True):
                st.metric("Total incidents", f"{kpis['total']:,}")
        with kpi2:
            avg_resp_min = round(kpis["avg_resp"] / 60, 1) if kpis["avg_resp"] else "—"
            with st.container(border=True):
                st.metric("Avg Response Time", f"{avg_resp_min} min")
        with kpi3:
            avg_disp_sec = int(kpis["avg_disp"]) if kpis["avg_disp"] else "—"
            with st.container(border=True):
                st.metric("Avg Dispatch Time", f"{avg_disp_sec} sec")
        with kpi4:
            with st.container(border=True):
                st.metric("Special Event Incidents", f"{kpis['special']:,}")

        st.write("")

        # ── Row 1: Borough ranking + Response time ────────────────────────────
        col1, col2 = st.columns(2)

        with col1:
            with st.container(border=True):
                st.markdown("**:material/bar_chart: Incidents by Borough**")
                df_rank = load_borough_rank(start_date, end_date, boroughs_tuple)
                st.altair_chart(
                    alt.Chart(df_rank).mark_bar().encode(
                        x=alt.X("total_incidents:Q", title="Total Incidents"),
                        y=alt.Y("borough:N", sort="-x", title=None),
                        color=alt.Color("borough:N", legend=None,
                            scale=alt.Scale(domain=BOROUGH_DOMAIN, range=BOROUGH_RANGE)),
                        tooltip=[
                            alt.Tooltip("borough:N", title="Borough"),
                            alt.Tooltip("total_incidents:Q", title="Incidents", format=","),
                            alt.Tooltip("rank:Q", title="Rank"),
                        ],
                    ).properties(height=CHART_HEIGHT),
                    use_container_width=True,
                )

        with col2:
            with st.container(border=True):
                st.markdown("**:material/timer: Avg Response Time by Borough (seconds)**")
                df_resp = load_avg_response(start_date, end_date, boroughs_tuple)
                melted = df_resp.melt(
                    id_vars="borough",
                    value_vars=["avg_dispatch_response_seconds", "avg_incident_response_seconds"],
                    var_name="metric", value_name="seconds",
                )
                melted["metric"] = melted["metric"].map({
                    "avg_dispatch_response_seconds": "Dispatch",
                    "avg_incident_response_seconds": "On-scene",
                })
                st.altair_chart(
                    alt.Chart(melted).mark_bar().encode(
                        x=alt.X("borough:N", title=None),
                        y=alt.Y("seconds:Q", title="Seconds"),
                        color=alt.Color("metric:N", title=None,
                            legend=alt.Legend(orient="bottom"),
                            scale=alt.Scale(domain=DUAL_DOMAIN, range=DUAL_RANGE)),
                        xOffset="metric:N",
                        tooltip=[
                            alt.Tooltip("borough:N", title="Borough"),
                            alt.Tooltip("metric:N", title="Metric"),
                            alt.Tooltip("seconds:Q", title="Seconds", format=",.0f"),
                        ],
                    ).properties(height=CHART_HEIGHT),
                    use_container_width=True,
                )

        st.write("")

        # ── Row 2: Response categories + MoM growth ───────────────────────────
        col3, col4 = st.columns(2)

        with col3:
            with st.container(border=True):
                st.markdown("**:material/speed: Response Speed by Borough**")
                df_cat = load_response_cat(start_date, end_date, boroughs_tuple)
                st.altair_chart(
                    alt.Chart(df_cat).mark_bar().encode(
                        x=alt.X("borough:N", title=None),
                        y=alt.Y("total_incidents:Q", title="Incidents", stack="normalize"),
                        color=alt.Color("response_category:N", title="Speed",
                            scale=alt.Scale(domain=RESPONSE_DOMAIN, range=RESPONSE_RANGE),
                            legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("borough:N", title="Borough"),
                            alt.Tooltip("response_category:N", title="Speed"),
                            alt.Tooltip("total_incidents:Q", title="Incidents", format=","),
                        ],
                    ).properties(height=CHART_HEIGHT),
                    use_container_width=True,
                )

        with col4:
            with st.container(border=True):
                st.markdown("**:material/trending_up: Month-over-Month Incident Volume**")
                df_mom = load_mom(start_date, end_date, boroughs_tuple)
                df_mom["month_label"] = df_mom["month_of_incident"].astype(int).map(MONTH_LABELS)
                df_mom["mom_pct_display"] = (df_mom["mom_pct_change"] * 100).round(1)
                st.altair_chart(
                    alt.Chart(df_mom).mark_bar().encode(
                        x=alt.X("month_label:N", sort=list(df_mom["month_label"]), title=None),
                        y=alt.Y("total_incidents:Q", title="Incidents"),
                        color=alt.condition(
                            alt.datum.mom_change > 0,
                            alt.value(MOM_POS),
                            alt.value(MOM_NEG),
                        ),
                        tooltip=[
                            alt.Tooltip("month_label:N", title="Month"),
                            alt.Tooltip("total_incidents:Q", title="Incidents", format=","),
                            alt.Tooltip("mom_change:Q", title="MoM change", format="+,"),
                            alt.Tooltip("mom_pct_display:Q", title="MoM %", format="+.1f"),
                        ],
                    ).properties(height=CHART_HEIGHT),
                    use_container_width=True,
                )

        st.write("")

        # ── Row 3: DOW heatmap + Special events ───────────────────────────────
        col5, col6 = st.columns(2)

        with col5:
            with st.container(border=True):
                st.markdown("**:material/calendar_today: Incidents by Day Of Week**")
                df_dow = load_dow(start_date, end_date, boroughs_tuple)
                df_dow["day_label"] = df_dow["day_of_week_of_incident"].map(DOW_LABELS)
                st.altair_chart(
                    alt.Chart(df_dow).mark_rect().encode(
                        x=alt.X("day_label:N", sort=["Sun","Mon","Tue","Wed","Thu","Fri","Sat"], title=None),
                        y=alt.Y("borough:N", title=None),
                        color=alt.Color("total_incidents:Q", title="Incidents",
                            scale=alt.Scale(scheme="reds")),
                        tooltip=[
                            alt.Tooltip("borough:N", title="Borough"),
                            alt.Tooltip("day_label:N", title="Day"),
                            alt.Tooltip("total_incidents:Q", title="Incidents", format=","),
                        ],
                    ).properties(height=CHART_HEIGHT),
                    use_container_width=True,
                )

        with col6:
            with st.container(border=True):
                st.markdown("**:material/local_hospital: Special Event Incidents by Borough**")
                df_special = load_special_events(start_date, end_date, boroughs_tuple)
                st.altair_chart(
                    alt.Chart(df_special).mark_bar().encode(
                        x=alt.X("total_incidents_during_special_events:Q",
                            title="Incidents during special events"),
                        y=alt.Y("borough:N", sort="-x", title=None),
                        color=alt.Color("borough:N", legend=None,
                            scale=alt.Scale(domain=BOROUGH_DOMAIN, range=BOROUGH_RANGE)),
                        tooltip=[
                            alt.Tooltip("borough:N", title="Borough"),
                            alt.Tooltip("total_incidents_during_special_events:Q",
                                title="Incidents", format=","),
                        ],
                    ).properties(height=CHART_HEIGHT),
                    use_container_width=True,
                )

        st.write("")

        # ── Row 4: Top call types ─────────────────────────────────────────────
        col7, col8 = st.columns(2)

        with col7:
            with st.container(border=True):
                st.markdown("**:material/call: Top Initial Call Types**")
                df_init = load_initial_call_types(start_date, end_date, boroughs_tuple)
                tab_chart, tab_table = st.tabs([":material/bar_chart: Chart", ":material/table: Table"])
                with tab_chart:
                    st.altair_chart(
                        alt.Chart(df_init.head(15)).mark_bar(color=ACCENT).encode(
                            x=alt.X("total_incidents:Q", title="Incidents"),
                            y=alt.Y("initial_call_type_desc:N", sort="-x", title=None),
                            tooltip=[
                                alt.Tooltip("initial_call_type_desc:N", title="Call type"),
                                alt.Tooltip("total_incidents:Q", title="Incidents", format=","),
                            ],
                        ).properties(height=CHART_HEIGHT),
                        use_container_width=True,
                    )
                with tab_table:
                    st.dataframe(df_init, hide_index=True, height=CHART_HEIGHT,
                        column_config={
                            "initial_call_type_desc": st.column_config.TextColumn("Call type"),
                            "total_incidents": st.column_config.NumberColumn("Incidents", format="%d"),
                        })

        with col8:
            with st.container(border=True):
                st.markdown("**:material/assignment_turned_in: Top Final Call Types**")
                df_final = load_final_call_types(start_date, end_date, boroughs_tuple)
                tab_chart2, tab_table2 = st.tabs([":material/bar_chart: Chart", ":material/table: Table"])
                with tab_chart2:
                    st.altair_chart(
                        alt.Chart(df_final.head(15)).mark_bar(color=ACCENT).encode(
                            x=alt.X("total_incidents:Q", title="Incidents"),
                            y=alt.Y("final_call_type_desc:N", sort="-x", title=None),
                            tooltip=[
                                alt.Tooltip("final_call_type_desc:N", title="Call type"),
                                alt.Tooltip("total_incidents:Q", title="Incidents", format=","),
                            ],
                        ).properties(height=CHART_HEIGHT),
                        use_container_width=True,
                    )
                with tab_table2:
                    st.dataframe(df_final, hide_index=True, height=CHART_HEIGHT,
                        column_config={
                            "final_call_type_desc": st.column_config.TextColumn("Call type"),
                            "total_incidents": st.column_config.NumberColumn("Incidents", format="%d"),
                        })

    except Exception as e:
        st.error(f"Could not load data: {e}", icon=":material/error:")
        st.caption("Make sure the DuckDB database exists and dbt models are materialized.")

# =============================================================================
# Chatbot tab
# =============================================================================

with tab_chatbot:
    st.markdown("# :material/chat: EMS Assistant")
    st.caption("Ask questions about NYC EMS incident data in plain English.")
    st.write("")

    if not RAG_READY:
        st.error(f"Could not initialise EMS Assistant: {_RAG_ERROR}", icon=":material/error:")
        st.stop()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Suggestion chips — shown only when chat is empty
    if not st.session_state.messages:
        suggestions = [
            "Which borough has the most incidents?",
            "What's the average response time by borough?",
            "How do incidents vary by day of the week?",
            "What are the top 5 emergency call types?",
        ]
        st.caption("Try asking:")
        s_cols = st.columns(len(suggestions))
        for i, s in enumerate(suggestions):
            if s_cols[i].button(s, use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": s})
                st.rerun()
        st.write("")

    # Render message history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Clear chat button
    if st.session_state.messages:
        st.write("")
        if st.button(":material/delete: Clear chat", type="secondary"):
            st.session_state.messages = []
            st.rerun()

    # Chat input
    if prompt := st.chat_input("Ask anything about NYC EMS data…"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                history = st.session_state.messages[:-1]  # everything before current question
                try:
                    answer = ask_ems(prompt, history)
                except Exception as exc:
                    answer = f"Sorry, I hit an error: {exc}"
            st.markdown(answer)

        st.session_state.messages.append({"role": "assistant", "content": answer})
