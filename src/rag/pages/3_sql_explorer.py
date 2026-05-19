"""
Page 3 — SQL Explorer
UPGRADED: Natural language to SQL (NL2SQL) added.

Two modes:
1. Natural Language mode — user types plain English, Groq generates SQL
2. Manual SQL mode — user writes SQL directly (existing behavior)

NL2SQL flow:
  User question → POST /api/v1/sql/ask → Groq generates SQL
  → Validates (SELECT only) → Executes on Snowflake → Returns results

Manual SQL flow (unchanged):
  User SQL → POST /api/v1/sql/query → Executes on Snowflake → Returns results
"""

import os
import warnings
import logging
from pathlib import Path

import requests
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

logging.getLogger("snowflake").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# ── Load .env ─────────────────────────────────────────────────────────────────
_this_file = Path(os.path.abspath(__file__))
for _parent in [
    _this_file.parent,
    _this_file.parent.parent,
    _this_file.parent.parent.parent,
    _this_file.parent.parent.parent.parent,
]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break

# ── API config ────────────────────────────────────────────────────────────────
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
API_V1       = f"{API_BASE_URL}/api/v1"


# ── API helpers ───────────────────────────────────────────────────────────────

def run_query(sql: str, database: str = "STOCKMARKETBATCH") -> pd.DataFrame:
    """Manual SQL → POST /api/v1/sql/query"""
    try:
        r = requests.post(
            f"{API_V1}/sql/query",
            json={"sql": sql, "database": database},
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json().get("rows", [])
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
    except requests.exceptions.ConnectionError:
        raise ConnectionError(f"Cannot connect to API at {API_BASE_URL}.")
    except requests.exceptions.Timeout:
        raise TimeoutError("API timed out after 30s")
    except requests.exceptions.HTTPError as e:
        try:
            detail = r.json().get("detail", str(e))
        except Exception:
            detail = str(e)
        raise Exception(f"API error: {detail}")
    except Exception as e:
        raise Exception(str(e))


def ask_natural_language(question: str, database: str = "STOCKMARKETBATCH") -> dict:
    """
    Natural language → POST /api/v1/sql/ask
    Returns full response including generated_sql and rows.
    """
    try:
        r = requests.post(
            f"{API_V1}/sql/ask",
            json={"question": question, "database": database},
            timeout=45,
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to API at {API_BASE_URL}."}
    except requests.exceptions.Timeout:
        return {"error": "API timed out — Groq may be slow, try again"}
    except requests.exceptions.HTTPError as e:
        try:
            detail = r.json().get("detail", str(e))
        except Exception:
            detail = str(e)
        return {"error": f"API error: {detail}"}
    except Exception as e:
        return {"error": str(e)}


# ── NL2SQL example questions ──────────────────────────────────────────────────
NL_EXAMPLES = [
    "Show me AAPL close price for the last 30 days",
    "Which stocks have a BUY signal right now?",
    "Top 5 stocks by average daily return",
    "What is TSLA's highest price ever?",
    "Show me all stocks with positive return yesterday",
    "Compare SMA-5 and SMA-20 for NVDA",
    "Which stocks had the most volatile day last week?",
    "Show me latest realtime data for all stocks",
]

# ── SQL Presets (existing behavior) ──────────────────────────────────────────
PRESETS = {
    "Top 5 by avg return": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT SYMBOL,
    ROUND(AVG(DAILY_RETURN_PCT), 4) AS AVG_RETURN,
    COUNT(*) AS TRADING_DAYS
FROM HISTORICAL_STOCK
GROUP BY SYMBOL
ORDER BY AVG_RETURN DESC
LIMIT 5;"""
    },
    "Golden cross (SMA crossover)": {
        "db":  "STOCKMARKETBATCH",
        "sql": """WITH CROSSOVERS AS (
    SELECT SYMBOL, DATE, SMA_5, SMA_20,
        LAG(SMA_5)  OVER (PARTITION BY SYMBOL ORDER BY DATE) AS PREV_SMA5,
        LAG(SMA_20) OVER (PARTITION BY SYMBOL ORDER BY DATE) AS PREV_SMA20
    FROM HISTORICAL_STOCK
    WHERE SMA_5 IS NOT NULL AND SMA_20 IS NOT NULL
)
SELECT SYMBOL, DATE,
    ROUND(SMA_5, 2)  AS SMA_5,
    ROUND(SMA_20, 2) AS SMA_20
FROM CROSSOVERS
WHERE PREV_SMA5 < PREV_SMA20 AND SMA_5 > SMA_20
ORDER BY DATE DESC
LIMIT 10;"""
    },
    "Buy/sell signals (dbt)": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT SYMBOL, TRADE_DATE, CLOSE_PRICE,
    DAILY_RETURN_PCT, SMA_SIGNAL, OVERALL_SIGNAL
FROM STOCK_PERFORMANCE
ORDER BY SYMBOL;"""
    },
    "Monthly volume by symbol": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT SYMBOL,
    DATE_TRUNC('month', DATE) AS MONTH,
    SUM(VOLUME)               AS TOTAL_VOLUME,
    ROUND(AVG(CLOSE_PRICE),2) AS AVG_CLOSE
FROM HISTORICAL_STOCK
GROUP BY SYMBOL, DATE_TRUNC('month', DATE)
ORDER BY SYMBOL, MONTH DESC
LIMIT 30;"""
    },
    "Data quality check": {
        "db":  "STOCKMARKETBATCH",
        "sql": """SELECT
    DATE_TRUNC('day', DATE)  AS BATCH_DATE,
    COUNT(*)                 AS TOTAL_ROWS,
    COUNT(DISTINCT SYMBOL)   AS SYMBOLS,
    SUM(CASE WHEN CLOSE_PRICE < 0        THEN 1 ELSE 0 END) AS NEG_PRICES,
    SUM(CASE WHEN HIGH_PRICE < LOW_PRICE THEN 1 ELSE 0 END) AS HIGH_LT_LOW
FROM HISTORICAL_STOCK
GROUP BY DATE_TRUNC('day', DATE)
ORDER BY BATCH_DATE DESC;"""
    },
    "Realtime latest window": {
        "db":  "STOCKMARKETSTREAM",
        "sql": """SELECT SYMBOL, WINDOW_START,
    ROUND(MA_15M, 2)         AS MA_15M,
    ROUND(MA_1H,  2)         AS MA_1H,
    ROUND(VOLATILITY_15M, 4) AS VOL_15M,
    VOLUME_SUM_1H
FROM REALTIME_STOCK
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SYMBOL ORDER BY WINDOW_START DESC
) = 1
ORDER BY SYMBOL;"""
    },
}

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Mode")
    mode = st.radio(
        "Query mode",
        ["Natural Language", "Manual SQL"],
        label_visibility="collapsed"
    )

    st.divider()

    if mode == "Natural Language":
        st.markdown("### Example questions")
        for q in NL_EXAMPLES:
            if st.button(q, key=f"nl_{q}", use_container_width=True):
                st.session_state["nl_question"] = q
                st.rerun()

    else:
        st.markdown("### Preset queries")
        for name, preset in PRESETS.items():
            if st.button(name, key=f"preset_{name}", use_container_width=True):
                st.session_state["sql_query"] = preset["sql"]
                st.session_state["sql_db"]    = preset["db"]
                st.rerun()

    st.divider()
    st.markdown("### Tables")
    st.caption("**STOCKMARKETBATCH**")
    for t in ["HISTORICAL_STOCK", "STOCK_DAILY_METRICS",
              "STOCK_PERFORMANCE"]:
        st.code(t, language=None)
    st.caption("**STOCKMARKETSTREAM**")
    for t in ["REALTIME_STOCK", "STG_REALTIME_STOCK"]:
        st.code(t, language=None)

    st.divider()
    st.markdown("### API")
    st.caption(f"Endpoint: `{API_BASE_URL}`")
    st.caption("NL2SQL: Groq → Snowflake")
    st.caption("Manual: Direct → Snowflake")


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔍 SQL Explorer")
st.caption(
    f"Query Snowflake in plain English or raw SQL · "
    f"Via FastAPI · API: `{API_BASE_URL}`"
)
st.divider()

# ── Database selector ─────────────────────────────────────────────────────────
db_options = ["STOCKMARKETBATCH", "STOCKMARKETSTREAM"]
default_db = st.session_state.get("sql_db", "STOCKMARKETBATCH")
db = st.selectbox(
    "Database",
    db_options,
    index=db_options.index(default_db)
)

# ══════════════════════════════════════════════════════════════════════════════
# MODE 1 — NATURAL LANGUAGE
# ══════════════════════════════════════════════════════════════════════════════
if mode == "Natural Language":
    st.subheader("Ask in plain English")
    st.caption(
        "Type any question about your stock data. "
        "Groq will generate the SQL, execute it, and show you both."
    )

    # Input
    default_q = st.session_state.get("nl_question", "")
    question = st.text_input(
        "Your question",
        value=default_q,
        placeholder="e.g. Show me AAPL close price for last 30 days",
        label_visibility="collapsed"
    )
    st.session_state["nl_question"] = question

    col_ask, _ = st.columns([1, 4])
    ask = col_ask.button("▶ Ask", type="primary", use_container_width=True)

    if ask and question.strip():
        with st.spinner("Groq is generating SQL · Executing on Snowflake..."):
            result = ask_natural_language(question.strip(), db)

        if "error" in result and result["error"]:
            st.error(f"Error: {result['error']}")

        else:
            # Show generated SQL — transparency is key for NL2SQL
            st.success(f"{result.get('row_count', 0):,} rows · {result.get('column_count', 0)} columns · {db}")

            with st.expander("🤖 Generated SQL", expanded=True):
                st.code(result.get("generated_sql", ""), language="sql")
                st.caption(
                    "This SQL was generated by Groq llama-3.3-70b from your question. "
                    "You can copy it to Manual SQL mode to edit."
                )

            # Show results
            rows = result.get("rows", [])
            if rows:
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Copy to manual SQL
                col_dl, col_copy = st.columns([1, 1])
                with col_dl:
                    st.download_button(
                        label="Download as CSV",
                        data=df.to_csv(index=False),
                        file_name="nl_query_result.csv",
                        mime="text/csv",
                    )
                with col_copy:
                    if st.button("Edit SQL manually", use_container_width=True):
                        st.session_state["sql_query"] = result.get("generated_sql", "")
                        st.session_state["sql_db"]    = db
                        st.rerun()
            else:
                st.info("Query returned no results.")

    elif not question.strip() and ask:
        st.warning("Please enter a question first.")

    else:
        # Empty state
        st.info(
            "Ask anything about your stock data in plain English.\n\n"
            "Examples from the sidebar, or type your own question."
        )

# ══════════════════════════════════════════════════════════════════════════════
# MODE 2 — MANUAL SQL (existing behavior, unchanged)
# ══════════════════════════════════════════════════════════════════════════════
else:
    st.subheader("Write SQL directly")

    default_sql = st.session_state.get(
        "sql_query",
        PRESETS["Top 5 by avg return"]["sql"]
    )
    sql = st.text_area(
        "SQL query",
        value=default_sql,
        height=200,
        placeholder="SELECT * FROM HISTORICAL_STOCK LIMIT 10;"
    )
    st.session_state["sql_query"] = sql

    col_run, _ = st.columns([1, 4])
    run = col_run.button("▶ Run query", type="primary", use_container_width=True)

    if run and sql.strip():
        with st.spinner(f"Running via FastAPI → Snowflake ({db})..."):
            try:
                df = run_query(sql, db)
                st.success(f"{len(df):,} rows · {len(df.columns)} columns · {db}")
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.download_button(
                    label     = "Download as CSV",
                    data      = df.to_csv(index=False),
                    file_name = "query_result.csv",
                    mime      = "text/csv",
                )
            except Exception as e:
                st.error(f"Query error: {str(e)}")
    else:
        st.info(
            "Select a preset from the sidebar or write your own SQL, "
            "then click Run query."
        )