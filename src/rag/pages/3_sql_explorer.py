"""
Page 3 — SQL Explorer
MIGRATED: Snowflake connection replaced with POST /api/v1/sql/query

What changed:
  - Removed snowflake.connector entirely
  - run_query() now calls POST /api/v1/sql/query instead of opening a DB connection
  - Database selector still works — passed as payload to the API
  - All presets unchanged — same SQL, different transport layer
  - Credentials completely removed from this file

Why this matters:
  SQL Explorer is now safe to share or deploy — no credentials exposed.
  The API validates and executes queries server-side.
  You can add query whitelisting or rate limiting at the API level later
  without touching this file at all.
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


# ── API helper ────────────────────────────────────────────────────────────────

def run_query(sql: str, database: str = "STOCKMARKETBATCH") -> pd.DataFrame:
    """
    Previously: opened a snowflake.connector connection, ran cursor.execute(),
    fetched rows, built a DataFrame manually.

    Now: POST to /api/v1/sql/query with the SQL and database name.
    The API handles the Snowflake connection — this file has zero DB knowledge.

    Same return type (pd.DataFrame) so all downstream UI code is unchanged.
    """
    try:
        r = requests.post(
            f"{API_V1}/sql/query",
            json={"sql": sql, "database": database},
            timeout=30,
        )
        r.raise_for_status()
        result = r.json()

        rows = result.get("rows", [])
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    except requests.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot connect to API at {API_BASE_URL}. "
            "Make sure FastAPI is running: python -m uvicorn api.main:app --reload --port 8000"
        )
    except requests.exceptions.Timeout:
        raise TimeoutError("API request timed out after 30s")
    except requests.exceptions.HTTPError as e:
        # Surface the API's error message, not a generic HTTP error
        try:
            detail = r.json().get("detail", str(e))
        except Exception:
            detail = str(e)
        raise Exception(f"API error: {detail}")
    except Exception as e:
        raise Exception(str(e))


# ── Presets — unchanged, same SQL queries ────────────────────────────────────
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
    st.markdown("### Preset queries")
    for name, preset in PRESETS.items():
        if st.button(name, key=f"preset_{name}", use_container_width=True):
            st.session_state["sql_query"] = preset["sql"]
            st.session_state["sql_db"]    = preset["db"]
            st.rerun()

    st.divider()
    st.markdown("### Tables")
    st.caption("**STOCKMARKETBATCH**")
    for t in ["HISTORICAL_STOCK","STG_HISTORICAL_STOCK",
              "STOCK_DAILY_METRICS","STOCK_PERFORMANCE"]:
        st.code(t, language=None)
    st.caption("**STOCKMARKETSTREAM**")
    for t in ["REALTIME_STOCK","STG_REALTIME_STOCK","STOCK_REALTIME_SUMMARY"]:
        st.code(t, language=None)

    st.divider()
    st.markdown("### API")
    st.caption(f"Endpoint: `{API_BASE_URL}`")
    st.caption("All queries routed via FastAPI")
    st.caption("No Snowflake credentials in Streamlit")

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔍 SQL Explorer")
st.caption(
    f"Query Snowflake via FastAPI · STOCKMARKETBATCH · STOCKMARKETSTREAM · "
    f"API: `{API_BASE_URL}`"
)
st.divider()

# ── Editor ────────────────────────────────────────────────────────────────────
db_options  = ["STOCKMARKETBATCH","STOCKMARKETSTREAM"]
default_db  = st.session_state.get("sql_db","STOCKMARKETBATCH")
db = st.selectbox("Database", db_options,
                  index=db_options.index(default_db))

default_sql = st.session_state.get("sql_query",
              PRESETS["Top 5 by avg return"]["sql"])
sql = st.text_area("SQL query", value=default_sql, height=200,
                   placeholder="SELECT * FROM HISTORICAL_STOCK LIMIT 10;")
st.session_state["sql_query"] = sql

col_run, _ = st.columns([1, 4])
run = col_run.button("▶ Run query", type="primary", use_container_width=True)

# ── Results ───────────────────────────────────────────────────────────────────
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
        "Select a preset from the sidebar or write your own SQL, then click Run query.\n\n"
        f"Queries are executed via FastAPI at `{API_BASE_URL}` — no direct DB connection."
    )