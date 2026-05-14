"""
Page 4 — RAG Intelligence Monitor
Medallion Architecture Dashboard

Reads from GOLD layer (intelligence_metrics) to show:
- Query volume per symbol
- Latency performance
- Semantic similarity scores (search quality)
- Sentiment distribution
- Response quality

Data flow:
  User query → FastAPI → BRONZE (RAW_INTELLIGENCE_LOGS)
  → dbt SILVER (stg_intelligence_logs)
  → dbt GOLD (intelligence_metrics) ← this page reads here
"""

import os
import warnings
import logging
from pathlib import Path

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

# ── API config ────────────────────────────────────────────────────────────────
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
API_V1       = f"{API_BASE_URL}/api/v1"


# ── API helper ────────────────────────────────────────────────────────────────

def sql_to_df(sql: str, database: str = "STOCKMARKETBATCH") -> pd.DataFrame:
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
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


def has_error(df: pd.DataFrame) -> bool:
    return "error" in df.columns


@st.cache_data(ttl=60, show_spinner=False)
def get_gold_metrics() -> pd.DataFrame:
    """Read from GOLD layer — intelligence_metrics table."""
    return sql_to_df("""
        SELECT
            LOG_DATE,
            SYMBOL,
            TOTAL_QUERIES,
            AVG_LATENCY_MS,
            MIN_LATENCY_MS,
            MAX_LATENCY_MS,
            SLOW_QUERIES,
            AVG_SIMILARITY_SCORE,
            MIN_SIMILARITY_SCORE,
            MAX_SIMILARITY_SCORE,
            POSITIVE_RESULTS,
            NEGATIVE_RESULTS,
            NEUTRAL_RESULTS,
            AVG_RESPONSE_LENGTH,
            MODEL
        FROM INTELLIGENCE_METRICS
        ORDER BY LOG_DATE DESC, TOTAL_QUERIES DESC
    """)


@st.cache_data(ttl=60, show_spinner=False)
def get_raw_logs() -> pd.DataFrame:
    """Read from BRONZE layer for recent activity."""
    return sql_to_df("""
        SELECT
            TIMESTAMP,
            QUESTION,
            SYMBOL,
            LATENCY_MS,
            LEFT(GROQ_RESPONSE, 150) AS RESPONSE_PREVIEW
        FROM RAW_INTELLIGENCE_LOGS
        ORDER BY TIMESTAMP DESC
        LIMIT 20
    """)


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔬 RAG Intelligence Monitor")
st.caption(
    "Medallion Architecture · BRONZE → SILVER → GOLD · "
    "Semantic similarity monitoring · Via FastAPI"
)
st.divider()

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading metrics from GOLD layer..."):
    df = get_gold_metrics()

if has_error(df):
    st.error(f"Could not load metrics: {df['error'].iloc[0]}")
    st.stop()

numeric_cols = [
    "TOTAL_QUERIES", "AVG_LATENCY_MS", "MIN_LATENCY_MS", "MAX_LATENCY_MS",
    "SLOW_QUERIES", "AVG_SIMILARITY_SCORE", "MIN_SIMILARITY_SCORE",
    "MAX_SIMILARITY_SCORE", "POSITIVE_RESULTS", "NEGATIVE_RESULTS",
    "NEUTRAL_RESULTS", "AVG_RESPONSE_LENGTH"
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

if df.empty:
    st.warning(
        "No data in INTELLIGENCE_METRICS yet.\n\n"
        "Ask some questions in Market Intelligence, then run:\n"
        "`dbt run --select stg_intelligence_logs intelligence_metrics`"
    )
    st.stop()


# ── Section 1 — Overall KPIs ──────────────────────────────────────────────────
st.subheader("Overall Pipeline Health")

total_queries    = int(df["TOTAL_QUERIES"].sum())
avg_latency      = float(df["AVG_LATENCY_MS"].mean())
avg_similarity   = float(df["AVG_SIMILARITY_SCORE"].mean())
total_slow       = int(df["SLOW_QUERIES"].sum())
total_positive   = int(df["POSITIVE_RESULTS"].sum())
total_negative   = int(df["NEGATIVE_RESULTS"].sum())
total_neutral    = int(df["NEUTRAL_RESULTS"].sum())
total_sentiment  = total_positive + total_negative + total_neutral

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric(
    "Total Queries",
    f"{total_queries}",
    "all time"
)
k2.metric(
    "Avg Latency",
    f"{avg_latency/1000:.1f}s",
    "lower is better"
)
k3.metric(
    "Avg Similarity Score",
    f"{avg_similarity:.3f}",
    "higher is better"
)
k4.metric(
    "Slow Queries (>8s)",
    f"{total_slow}",
    f"{total_slow/total_queries*100:.0f}% of total" if total_queries > 0 else "0%"
)
k5.metric(
    "Positive Sentiment",
    f"{total_positive/total_sentiment*100:.0f}%" if total_sentiment > 0 else "N/A",
    f"{total_positive} articles"
)


# ── Section 2 — Similarity Score by Symbol ───────────────────────────────────
st.divider()
st.subheader("Semantic Similarity Score by Symbol")
st.caption(
    "Measures how relevant Pinecone's search results are to each question. "
    "Higher = better search quality. Drop indicates index degradation."
)

sim_by_symbol = df.groupby("SYMBOL").agg(
    AVG_SIMILARITY=("AVG_SIMILARITY_SCORE", "mean"),
    MIN_SIMILARITY=("MIN_SIMILARITY_SCORE", "min"),
    MAX_SIMILARITY=("MAX_SIMILARITY_SCORE", "max"),
    TOTAL_QUERIES=("TOTAL_QUERIES", "sum"),
).reset_index().sort_values("AVG_SIMILARITY", ascending=False)

col_chart, col_table = st.columns([2, 1])

with col_chart:
    st.bar_chart(
        sim_by_symbol.set_index("SYMBOL")["AVG_SIMILARITY"],
        use_container_width=True,
        height=300,
    )

with col_table:
    st.dataframe(
        sim_by_symbol.rename(columns={
            "SYMBOL": "Symbol",
            "AVG_SIMILARITY": "Avg Score",
            "MIN_SIMILARITY": "Min",
            "MAX_SIMILARITY": "Max",
            "TOTAL_QUERIES": "Queries",
        }).style.format({
            "Avg Score": "{:.3f}",
            "Min": "{:.3f}",
            "Max": "{:.3f}",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ── Section 3 — Latency Analysis ─────────────────────────────────────────────
st.divider()
st.subheader("Response Latency Analysis")
st.caption("Time taken per query. Target: <8 seconds. SLOW = >8 seconds.")

lat_by_symbol = df.groupby("SYMBOL").agg(
    AVG_LATENCY=("AVG_LATENCY_MS", "mean"),
    MIN_LATENCY=("MIN_LATENCY_MS", "min"),
    MAX_LATENCY=("MAX_LATENCY_MS", "max"),
    SLOW_QUERIES=("SLOW_QUERIES", "sum"),
).reset_index().sort_values("AVG_LATENCY", ascending=True)

# Convert ms to seconds for readability
lat_by_symbol["AVG_LATENCY_S"] = lat_by_symbol["AVG_LATENCY"] / 1000
lat_by_symbol["MAX_LATENCY_S"] = lat_by_symbol["MAX_LATENCY"] / 1000

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Average Latency by Symbol (seconds)**")
    st.bar_chart(
        lat_by_symbol.set_index("SYMBOL")["AVG_LATENCY_S"],
        use_container_width=True,
        height=250,
    )

with col2:
    st.markdown("**Latency Breakdown**")
    display_df = lat_by_symbol[["SYMBOL", "AVG_LATENCY_S", "MAX_LATENCY_S", "SLOW_QUERIES"]].copy()
    display_df.columns = ["Symbol", "Avg (s)", "Max (s)", "Slow Queries"]
    display_df["Avg (s)"] = display_df["Avg (s)"].round(1)
    display_df["Max (s)"] = display_df["Max (s)"].round(1)
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ── Section 4 — Sentiment Distribution ───────────────────────────────────────
st.divider()
st.subheader("News Sentiment Distribution")
st.caption(
    "Sentiment of top Pinecone result per query. "
    "Reflects market mood in retrieved news articles."
)

sentiment_data = pd.DataFrame({
    "Sentiment": ["Positive", "Negative", "Neutral"],
    "Count": [total_positive, total_negative, total_neutral],
    "Percentage": [
        f"{total_positive/total_sentiment*100:.1f}%" if total_sentiment > 0 else "0%",
        f"{total_negative/total_sentiment*100:.1f}%" if total_sentiment > 0 else "0%",
        f"{total_neutral/total_sentiment*100:.1f}%" if total_sentiment > 0 else "0%",
    ]
})

col_s1, col_s2, col_s3, col_s4 = st.columns(4)
with col_s1:
    pct = total_positive/total_sentiment*100 if total_sentiment > 0 else 0
    st.metric("📈 Positive", f"{total_positive}", f"{pct:.1f}%")
with col_s2:
    pct = total_negative/total_sentiment*100 if total_sentiment > 0 else 0
    st.metric("📉 Negative", f"{total_negative}", f"{pct:.1f}%")
with col_s3:
    pct = total_neutral/total_sentiment*100 if total_sentiment > 0 else 0
    st.metric("➖ Neutral", f"{total_neutral}", f"{pct:.1f}%")
with col_s4:
    st.metric("📰 Total Articles", f"{total_sentiment}")

# Sentiment by symbol
sent_by_symbol = df.groupby("SYMBOL").agg(
    POSITIVE=("POSITIVE_RESULTS", "sum"),
    NEGATIVE=("NEGATIVE_RESULTS", "sum"),
    NEUTRAL=("NEUTRAL_RESULTS", "sum"),
).reset_index()

st.bar_chart(
    sent_by_symbol.set_index("SYMBOL")[["POSITIVE", "NEGATIVE", "NEUTRAL"]],
    use_container_width=True,
    height=250,
)


# ── Section 5 — Full GOLD Table ───────────────────────────────────────────────
st.divider()
st.subheader("GOLD Layer — Full Metrics Table")
st.caption("intelligence_metrics dbt model · Aggregated daily per symbol")

st.dataframe(
    df.rename(columns={
        "LOG_DATE":            "Date",
        "SYMBOL":              "Symbol",
        "TOTAL_QUERIES":       "Queries",
        "AVG_LATENCY_MS":      "Avg Latency (ms)",
        "SLOW_QUERIES":        "Slow",
        "AVG_SIMILARITY_SCORE":"Avg Similarity",
        "MAX_SIMILARITY_SCORE":"Max Similarity",
        "POSITIVE_RESULTS":    "Positive",
        "NEGATIVE_RESULTS":    "Negative",
        "NEUTRAL_RESULTS":     "Neutral",
        "AVG_RESPONSE_LENGTH": "Avg Response Len",
        "MODEL":               "Model",
    }),
    use_container_width=True,
    hide_index=True,
)


# ── Section 6 — Recent Queries (BRONZE) ───────────────────────────────────────
st.divider()
st.subheader("Recent Queries — BRONZE Layer")
st.caption("Last 20 raw intelligence queries · RAW_INTELLIGENCE_LOGS")

with st.spinner("Loading recent queries..."):
    df_raw = get_raw_logs()

if not has_error(df_raw) and not df_raw.empty:
    st.dataframe(
        df_raw.rename(columns={
            "TIMESTAMP":        "Timestamp",
            "QUESTION":         "Question",
            "SYMBOL":           "Symbol",
            "LATENCY_MS":       "Latency (ms)",
            "RESPONSE_PREVIEW": "Response Preview",
        }),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No recent queries found.")


# ── Refresh ───────────────────────────────────────────────────────────────────
st.divider()
col_info, col_btn = st.columns([3, 1])
with col_info:
    st.caption(
        "GOLD layer cached 60s · "
        "Run `dbt run --select intelligence_metrics` to refresh aggregates"
    )
with col_btn:
    if st.button("Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()