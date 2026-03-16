"""
RAG module for the EMS Assistant chatbot.

Loads dbt YAML metadata into ChromaDB for schema-aware retrieval, then
answers questions by combining retrieved context with Claude's tool-use
capability to run live SQL against the DuckDB database.
"""

import os
import re
import yaml
import duckdb
import chromadb
import anthropic
from dotenv import load_dotenv

load_dotenv()

_BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_BASE, "data", "raw.duckdb")
MODELS_PATH = os.path.join(_BASE, "dbt", "models")

# =============================================================================
# ChromaDB — load all dbt YAML docs once at import time
# =============================================================================

_chroma = chromadb.Client()
_collection = _chroma.create_collection("ems_metadata")

_docs: list[str] = []
for _root, _dirs, _files in os.walk(MODELS_PATH):
    for _file in _files:
        if _file.endswith((".yml", ".yaml")):
            with open(os.path.join(_root, _file)) as _f:
                _data = yaml.safe_load(_f) or {}
            for _model in _data.get("models", []):
                _text = (
                    f"Model: {_model['name']}\n"
                    f"Description: {_model.get('description', '')}\n"
                )
                for _col in _model.get("columns", []):
                    _text += f"  Column: {_col['name']}: {_col.get('description', '')}\n"
                _docs.append(_text)

for _i, _doc in enumerate(_docs):
    _collection.add(documents=[_doc], ids=[str(_i)])

# =============================================================================
# Anthropic client
# =============================================================================

_anthropic = anthropic.Anthropic()

SYSTEM_PROMPT = """\
You are an expert analyst for NYC Emergency Medical Services (EMS) incident data.

Dataset: ~1,079,491 incidents across NYC's 5 boroughs (Jan–Aug 2025).
Main table: test.int_enrichment (DuckDB — dialect is mostly standard SQL)

Key columns in test.int_enrichment:
- incident_datetime (TIMESTAMP)
- borough (VARCHAR): BRONX, BROOKLYN, MANHATTAN, QUEENS, RICHMOND / STATEN ISLAND
- dispatch_response_seconds_qy (DOUBLE): seconds from call to dispatch
- incident_response_seconds_qy (DOUBLE): seconds from call to on-scene arrival
- valid_dispatch_rspns_time_indc (VARCHAR): filter = 'Y' for valid dispatch times
- valid_incident_rspns_time_indc (VARCHAR): filter = 'Y' for valid response times
- response_category (VARCHAR): Fast / Moderate / Slow
- initial_call_type_desc (VARCHAR): initial emergency classification
- final_call_type_desc (VARCHAR): resolved call type
- special_event_indicator (VARCHAR): 'Y' if incident during a special event
- day_of_week_of_incident (INTEGER): 0 = Sunday … 6 = Saturday
- month_of_incident (INTEGER): 1–12
- year_of_incident (INTEGER)

Rules:
- Always filter valid_dispatch_rspns_time_indc = 'Y' when querying dispatch times.
- Always filter valid_incident_rspns_time_indc = 'Y' when querying response times.
- Use the run_query tool whenever you need specific numbers from the data.
- Keep answers concise and friendly. Format numbers with commas. Use seconds or minutes as appropriate for time values.
"""

_TOOLS = [
    {
        "name": "run_query",
        "description": (
            "Execute a DuckDB SQL query against the EMS database and return the results "
            "as a formatted table. Use this to get specific numbers, counts, averages, "
            "rankings, or any data needed to answer the user's question."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A valid DuckDB SQL query against test.int_enrichment",
                }
            },
            "required": ["sql"],
        },
    }
]


# =============================================================================
# Public API
# =============================================================================

def retrieve_context(question: str) -> str:
    """Return the top-3 most relevant dbt schema docs for the question."""
    results = _collection.query(query_texts=[question], n_results=3)
    return "\n\n".join(results["documents"][0])


def _run_query(sql: str) -> str:
    """Execute SQL against DuckDB, return results as a string."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute(sql).df()
        con.close()
        if df.empty:
            return "Query returned no results."
        # Limit output to avoid overwhelming the context window
        if len(df) > 50:
            return df.head(50).to_string(index=False) + f"\n… ({len(df)} rows total, showing first 50)"
        return df.to_string(index=False)
    except Exception as exc:
        return f"Query error: {exc}"


def ask_ems(question: str, history: list[dict]) -> str:
    """
    Full pipeline: retrieve schema context → Claude with tool use → execute SQL → final answer.

    Args:
        question: the user's natural-language question
        history:  list of prior turns as {"role": "user"|"assistant", "content": str}

    Returns:
        The assistant's final text answer.
    """
    context = retrieve_context(question)
    system = SYSTEM_PROMPT + f"\n\nRelevant dbt schema context:\n{context}"

    messages = [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": question})

    # Tool-use loop — Claude may call run_query multiple times before answering
    while True:
        response = _anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=system,
            tools=_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "tool_use":
            tool_results = []
            # Serialize content blocks to plain dicts to avoid Pydantic v2 by_alias errors
            assistant_content = [
                block.model_dump() for block in response.content
            ]
            for block in response.content:
                if block.type == "tool_use":
                    result = _run_query(block.input["sql"])
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results})
        else:
            return "".join(
                block.text for block in response.content if hasattr(block, "text")
            )
