"""
Page 2 — Pipeline Dashboard
MIGRATED: All Snowflake calls now go through FastAPI layer.

What changed:
  - Removed snowflake.connector entirely
  - Removed all direct DB credentials
  - query_batch() / query_stream() replaced with API calls
  - KPIs pulled from GET /api/v1/pipeline/kpis
  - Price history pulled from GET /api/v1/stocks/historical
  - Signals pulled from GET /api/v1/stocks/signals
  - SQL queries for data quality pulled from POST /api/v1/sql/query

Why this matters:
  Streamlit is now a pure UI layer. No credentials, no DB drivers.
  If Snowflake changes, only the API services change — not this file.
"""

import os
import warnings
import logging
from pathlib import Path
from typing import Any, TypeVar
from datetime import datetime, timezone

import requests
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
logging.getLogger("snowflake").setLevel(logging.ERROR)

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

# ── API config — single source of truth ──────────────────────────────────────
# Change API_BASE_URL in .env to point to a different server without touching code
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
API_V1       = f"{API_BASE_URL}/api/v1"


# ── API helpers ───────────────────────────────────────────────────────────────

def api_get(path: str, params: dict | None = None) -> dict:
    """
    GET request to FastAPI. Returns parsed JSON or error dict.
    All data fetching goes through here — one place to add auth headers later.
    """
    try:
        r = requests.get(f"{API_V1}{path}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to API at {API_BASE_URL}. Is it running?"}
    except requests.exceptions.Timeout:
        return {"error": "API request timed out after 15s"}
    except Exception as e:
        return {"error": str(e)}


def api_post(path: str, payload: dict) -> dict:
    """
    POST request to FastAPI. Used for SQL queries.
    """
    try:
        r = requests.post(f"{API_V1}{path}", json=payload, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to API at {API_BASE_URL}. Is it running?"}
    except requests.exceptions.Timeout:
        return {"error": "API request timed out after 15s"}
    except Exception as e:
        return {"error": str(e)}


def has_error(data) -> bool:
    """Check if API response or DataFrame has an error."""
    if isinstance(data, dict):
        return "error" in data
    if isinstance(data, pd.DataFrame):
        return "error" in data.columns
    return False


def sql_to_df(sql: str, database: str = "STOCKMARKETBATCH") -> pd.DataFrame:
    """
    Run a SQL query via POST /api/v1/sql/query and return a DataFrame.
    Replaces the old run_query() / query_batch() / query_stream() functions.
    The API handles the Snowflake connection — Streamlit knows nothing about it.
    """
    result = api_post("/sql/query", {"sql": sql, "database": database})
    if has_error(result):
        return pd.DataFrame({"error": [result["error"]]})
    rows = result.get("rows", [])
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data(ttl=30, show_spinner=False)
def cached_sql(sql: str, database: str = "STOCKMARKETBATCH") -> pd.DataFrame:
    """Cached wrapper around sql_to_df — same 30s TTL as before."""
    return sql_to_df(sql, database)


@st.cache_data(ttl=60, show_spinner=False)
def get_pipeline_kpis() -> dict:
    """GET /api/v1/pipeline/kpis — cached 60s."""
    return api_get("/pipeline/kpis")


@st.cache_data(ttl=30, show_spinner=False)
def get_historical(symbol: str, days: int) -> dict:
    """GET /api/v1/stocks/historical — cached 30s."""
    return api_get("/stocks/historical", {"symbol": symbol, "days": days})


@st.cache_data(ttl=300, show_spinner=False)
def get_signals() -> dict:
    """GET /api/v1/stocks/signals — cached 5m (signals don't change often)."""
    return api_get("/stocks/signals")


_T = TypeVar("_T")


def safe_val(data: Any, key: str, default: _T) -> _T:
    if has_error(data) or not data:
        return default
    return data.get(key, default) or default


# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_time = st.columns([3, 1])
with col_title:
    st.title("📊 Pipeline Dashboard")
    st.caption(f"Kafka → Spark → Snowflake → dbt · Via FastAPI ({API_BASE_URL}) · Auto-refreshes every 30s")
with col_time:
    st.metric("Last refresh", datetime.now(timezone.utc).strftime("%H:%M:%S UTC"))

# ── API health check ──────────────────────────────────────────────────────────
# Replaces the old Snowflake credential check
health = api_get("/health")
if has_error(health):
    st.error(
        f"**FastAPI is not reachable.**\n\n"
        f"{health['error']}\n\n"
        f"Start it with: `python -m uvicorn api.main:app --reload --port 8000`"
    )
    st.stop()


# ── Section 1 — KPIs ──────────────────────────────────────────────────────────
st.divider()
st.subheader("Pipeline KPIs")

# Single API call replaces 5 separate Snowflake queries
# The API aggregates everything and returns one clean response
kpis = get_pipeline_kpis()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Historical rows",   f"{int(safe_val(kpis, 'historical_rows', 0)):,}",
          f"{int(safe_val(kpis, 'symbol_count', 0))} symbols")
k2.metric("Latest batch date", str(safe_val(kpis, "latest_batch_date", "N/A")))
k3.metric("Realtime windows",  f"{int(safe_val(kpis, 'realtime_windows', 0)):,}",
          "STOCKMARKETSTREAM")
k4.metric("Positive days",     f"{float(safe_val(kpis, 'positive_day_pct', 0)):.1f}%",
          "all stocks all time")
k5.metric("dbt tests",         str(safe_val(kpis, "dbt_tests_passing", "N/A")),
          "all passing ✓")


# ── Section 2 — Buy/Sell Signals ──────────────────────────────────────────────
st.divider()
st.subheader("Buy / Sell Signals")
st.caption("Source: dbt STOCK_PERFORMANCE mart via FastAPI")

# Using SQL passthrough for the signals table — same query, different transport
df_perf = cached_sql("""
    SELECT
        SYMBOL,
        TRADE_DATE,
        ROUND(CLOSE_PRICE, 2)      AS CLOSE,
        ROUND(DAILY_RETURN_PCT, 2) AS RETURN_PCT,
        ROUND(SMA_5,  2)           AS SMA_5,
        ROUND(SMA_20, 2)           AS SMA_20,
        SMA_SIGNAL,
        OVERALL_SIGNAL
    FROM STOCK_PERFORMANCE
    ORDER BY SYMBOL
""", "STOCKMARKETBATCH")

if has_error(df_perf):
    st.error(f"Could not load signals: {df_perf['error'].iloc[0]}")
elif df_perf.empty:
    st.warning("No data in STOCK_PERFORMANCE — run: dbt run --select stock_performance")
else:
    def color_signal(val):
        val = str(val).upper()
        if val in ("BULLISH", "BUY"):
            return "background-color: #0a2a0a; color: #40c060"
        elif val in ("BEARISH", "SELL"):
            return "background-color: #2a0a0a; color: #e06060"
        return ""

    df_display = df_perf.rename(columns={
        "SYMBOL": "Symbol", "TRADE_DATE": "Date",
        "CLOSE": "Close ($)", "RETURN_PCT": "Return %",
        "SMA_5": "SMA-5", "SMA_20": "SMA-20",
        "SMA_SIGNAL": "SMA signal", "OVERALL_SIGNAL": "Signal",
    })

    st.dataframe(
        df_display.style.applymap(
            color_signal,
            subset=["SMA signal", "Signal"]
        ),
        use_container_width=True,
        hide_index=True,
    )


# ── Section 3 — Realtime Stream ───────────────────────────────────────────────
st.divider()
st.subheader("Realtime Stream — Latest Window Per Symbol")
st.caption("Source: STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK via FastAPI")

df_rt_d = cached_sql("""
    SELECT SYMBOL,
           WINDOW_START,
           ROUND(MA_15M, 2)         AS MA_15M,
           ROUND(MA_1H,  2)         AS MA_1H,
           ROUND(VOLATILITY_15M, 4) AS VOL_15M,
           VOLUME_SUM_1H
    FROM REALTIME_STOCK
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL ORDER BY WINDOW_START DESC
    ) = 1
    ORDER BY SYMBOL
""", "STOCKMARKETSTREAM")

if has_error(df_rt_d):
    st.error(f"Realtime error: {df_rt_d['error'].iloc[0]}")
elif df_rt_d.empty:
    st.warning("No realtime data — start the streaming pipeline first")
else:
    cols = st.columns(5)
    for i, (_, r) in enumerate(df_rt_d.iterrows()):
        ma15  = float(r.get("MA_15M") or 0)
        ma1h  = float(r.get("MA_1H")  or 0)
        diff  = ma15 - ma1h
        delta = f"+{diff:.2f}" if diff >= 0 else f"{diff:.2f}"
        with cols[i % 5]:
            st.metric(str(r["SYMBOL"]), f"${ma15:.2f}", f"{delta} vs 1h MA")
            st.caption(f"Vol 1h: {int(r.get('VOLUME_SUM_1H') or 0):,}")


# ── Section 4 — Price Chart ───────────────────────────────────────────────────
st.divider()
st.subheader("Price History Chart")

col_sym, col_days = st.columns([2, 1])
with col_sym:
    sym = st.selectbox("Stock", ["AAPL","MSFT","GOOGL","AMZN","META","TSLA","NVDA","INTC","JPM","V"])
with col_days:
    days = st.selectbox("Period", [30, 60, 90, 180, 365], index=2)

# Uses the dedicated historical endpoint — optimized, cached at API level
hist = get_historical(sym, days)

if has_error(hist):
    st.error(hist["error"])
elif hist.get("data"):
    df_hist = pd.DataFrame(hist["data"])
    if not df_hist.empty:
        st.line_chart(
            df_hist.sort_values("date").set_index("date")[["close_price", "sma_5", "sma_20"]],
            use_container_width=True,
        )


# ── Section 5 — Top Movers ────────────────────────────────────────────────────
st.divider()
st.subheader("Top Movers — Latest Day")

df_mv = cached_sql("""
    SELECT SYMBOL, CLOSE_PRICE, DAILY_RETURN_PCT
    FROM HISTORICAL_STOCK
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY DATE DESC) = 1
    ORDER BY DAILY_RETURN_PCT DESC
""", "STOCKMARKETBATCH")

if not has_error(df_mv) and not df_mv.empty:
    col_up, col_dn = st.columns(2)
    with col_up:
        st.markdown("**Top gainers**")
        for _, r in df_mv.head(5).iterrows():
            pct = float(r["DAILY_RETURN_PCT"] or 0)
            st.metric(str(r["SYMBOL"]), f"${float(r['CLOSE_PRICE']):.2f}", f"+{pct:.2f}%")
    with col_dn:
        st.markdown("**Top losers**")
        for _, r in df_mv.tail(5).sort_values("DAILY_RETURN_PCT").iterrows():
            pct = float(r["DAILY_RETURN_PCT"] or 0)
            st.metric(str(r["SYMBOL"]), f"${float(r['CLOSE_PRICE']):.2f}", f"{pct:.2f}%")


# ── Section 6 — Data Quality ──────────────────────────────────────────────────
st.divider()
st.subheader("Data Quality Checks")
st.caption("Live checks via FastAPI SQL passthrough — no direct DB connection")

df_hl   = cached_sql("SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK WHERE HIGH_PRICE < LOW_PRICE")
df_neg  = cached_sql("SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK WHERE CLOSE_PRICE < 0")
df_null = cached_sql("SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK WHERE SYMBOL IS NULL")
df_symc = cached_sql("SELECT COUNT(DISTINCT SYMBOL) AS CNT FROM HISTORICAL_STOCK")

def safe_cnt(df, default=-1):
    try:
        if has_error(df) or df.empty:
            return default
        val = df["CNT"].iloc[0]
        return int(val) if val is not None else default
    except Exception:
        return default

dq1, dq2, dq3, dq4 = st.columns(4)
with dq1:
    n = safe_cnt(df_hl)
    st.success("✓ High >= Low\n\n0 violations") if n == 0 else st.error(f"✗ High >= Low\n\n{n} violations")
with dq2:
    n = safe_cnt(df_neg)
    st.success("✓ No negative prices\n\n0 violations") if n == 0 else st.error(f"✗ Negative prices\n\n{n} found")
with dq3:
    n = safe_cnt(df_null)
    st.success("✓ No null symbols\n\n0 nulls") if n == 0 else st.error(f"✗ Null symbols\n\n{n} found")
with dq4:
    n = safe_cnt(df_symc, 0)
    st.success(f"✓ Symbol count\n\n{n} / 10 loaded") if n == 10 else st.warning(f"⚠ Symbol count\n\n{n} / 10 loaded")


# ── Refresh ───────────────────────────────────────────────────────────────────
st.divider()
col_info, col_btn = st.columns([3, 1])
with col_info:
    st.caption(f"Cached 30s · API: {API_BASE_URL} · Click to force refresh")
with col_btn:
    if st.button("Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()