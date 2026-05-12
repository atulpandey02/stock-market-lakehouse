"""
Page 1 — Market Intelligence (RAG Chat)
MIGRATED: All backend calls now go through FastAPI layer.

What changed:
  - Removed snowflake.connector, pinecone, sentence-transformers entirely
  - get_stock_metrics_from_snowflake() → POST /api/v1/intelligence/query
  - retrieve_from_pinecone() + generate_with_groq() → POST /api/v1/intelligence/query
  - render_pipeline_metrics() — unchanged UI, now receives API data
  - get_index_stats() → GET /api/v1/health (pinecone status from health endpoint)
  - All credentials removed from this file
"""

import os
import sys
import warnings
import logging
import requests
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# ── Path setup ────────────────────────────────────────────────────────────────
_this_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir  = os.path.dirname(_this_dir)
sys.path.insert(0, _root_dir)

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

def api_get(path: str, params: dict |None = None) -> dict:
    try:
        r = requests.get(f"{API_V1}{path}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to API at {API_BASE_URL}. Is it running?"}
    except requests.exceptions.Timeout:
        return {"error": "API timed out after 15s"}
    except Exception as e:
        return {"error": str(e)}


def api_post(path: str, payload: dict) -> dict:
    try:
        r = requests.post(f"{API_V1}{path}", json=payload, timeout=45)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to API at {API_BASE_URL}. Is it running?"}
    except requests.exceptions.Timeout:
        return {"error": "API timed out — Groq may be slow, try again"}
    except Exception as e:
        return {"error": str(e)}


# ── Data functions ────────────────────────────────────────────────────────────

def query_intelligence(question: str, symbol: str | None = None) -> dict:
    """
    Single API call replacing:
      - get_stock_metrics_from_snowflake()
      - retrieve_from_pinecone()
      - generate_with_groq()

    Returns: { answer, sources, pipeline_metrics }
    """
    payload = {"question": question}
    if symbol and symbol != "All Stocks":
        payload["symbol"] = symbol
    return api_post("/intelligence/query", payload)


def get_index_stats() -> dict:
    """
    Previously connected directly to Pinecone.
    Now reads from the health endpoint.
    """
    result = api_get("/health")
    if "error" in result:
        return {"total_vectors": 0, "dimension": 384, "status": f"Error: {result['error']}"}
    services = result.get("services", [])
    for svc in services:
        if svc.get("name") == "pinecone":
            if svc.get("status") == "ok":
                return {"total_vectors": "N/A", "dimension": 384, "status": "Connected ✓"}
            else:
                return {"total_vectors": 0, "dimension": 384, "status": f"Error: {svc.get('message')}"}
    return {"total_vectors": 0, "dimension": 384, "status": "Unknown"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_str(val, default="") -> str:
    return str(val) if val is not None else default

def safe_float(val, default=0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default

def safe_date(val) -> str:
    s = safe_str(val)
    return s[:10] if s else "N/A"


# ── UI — render pipeline metrics card ────────────────────────────────────────

def render_pipeline_metrics(metrics: dict, symbol: str):
    if not metrics or "error" in metrics:
        return

    # API returns lowercase keys — handle both old (uppercase) and new (lowercase)
    signal  = safe_str(metrics.get("overall_signal") or metrics.get("OVERALL_SIGNAL"), "N/A")
    close   = safe_float(metrics.get("close_price")  or metrics.get("CLOSE"),          0.0)
    sma5    = safe_float(metrics.get("sma_5")         or metrics.get("SMA_5"),          0.0)
    sma20   = safe_float(metrics.get("sma_20")        or metrics.get("SMA_20"),         0.0)
    ret_pct = safe_float(metrics.get("daily_return_pct") or metrics.get("RETURN_PCT"),  0.0)
    date    = safe_str(metrics.get("trade_date")      or metrics.get("TRADE_DATE"),     "")

    signal_color = (
        "🟢" if signal.upper() in ("BULLISH", "BUY")  else
        "🔴" if signal.upper() in ("BEARISH", "SELL") else
        "🟡"
    )

    with st.expander(
        f"📊 Pipeline Data — {symbol} · {signal_color} {signal} · from dbt STOCK_PERFORMANCE",
        expanded=True
    ):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Close",  f"${close}")
        c2.metric("SMA-5",  f"${sma5}")
        c3.metric("SMA-20", f"${sma20}")
        c4.metric("Return", f"{ret_pct}%", delta=f"{ret_pct}%", delta_color="normal")
        st.caption(
            f"Source: Snowflake → dbt STOCK_PERFORMANCE mart · "
            f"Via FastAPI · As of: {str(date)[:10]}"
        )


def render_sources(chunks: list):
    if not chunks:
        return
    with st.expander(f"📰 News Sources ({len(chunks)})"):
        for i, src in enumerate(chunks, 1):
            sent  = safe_str(src.get("sentiment"), "Neutral")
            score = safe_float(src.get("score"),   0.0)
            icon  = "📈" if sent == "Positive" else "📉" if sent == "Negative" else "➖"
            pub   = safe_date(src.get("published_at"))
            url   = safe_str(src.get("url"), "#")
            title = safe_str(src.get("title"), "N/A")

            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.markdown(f"**[{i}] {title}**")
                st.caption(
                    f"{safe_str(src.get('symbol'))} · "
                    f"{safe_str(src.get('source'))} · "
                    f"{pub}"
                )
                if url and url != "#":
                    st.markdown(f"[View article →]({url})")
            with col_b:
                st.metric("Score", f"{score:.3f}")
                st.caption(f"{icon} {sent}")

            if i < len(chunks):
                st.divider()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Filter")
    stocks = ["All Stocks","AAPL","MSFT","GOOGL","AMZN",
              "META","TSLA","NVDA","INTC","JPM","V"]
    selected_stock = st.selectbox("Stock", stocks, label_visibility="collapsed")

    st.divider()
    st.markdown("### Quick questions")
    ticker = selected_stock if selected_stock != "All Stocks" else "AAPL"
    for q in [
        f"What is the outlook for {ticker}?",
        f"Should I be concerned about {ticker}?",
        "What is the current market sentiment?",
        "Any supply chain concerns for tech stocks?",
        "What are analysts saying about earnings?",
    ]:
        if st.button(q, key=f"qq_{q}", use_container_width=True):
            st.session_state["quick_q"] = q
            st.rerun()

    st.divider()
    st.markdown("### Data sources")
    st.caption("📊 Pipeline: FastAPI → Snowflake → dbt")
    st.caption("📰 News: FastAPI → Pinecone")
    st.caption("🧠 LLM: FastAPI → Groq llama-3.3-70b-versatile")
    st.caption(f"🔌 API: `{API_BASE_URL}`")

    st.divider()
    if st.button("Check Pinecone status", use_container_width=True):
        with st.spinner("Checking..."):
            stats = get_index_stats()
        st.metric("Vectors stored", f"{stats['total_vectors']}")
        st.caption(f"Dim: {stats['dimension']} · {stats['status']}")

    st.divider()
    if st.button("Clear chat", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()


# ── Header ────────────────────────────────────────────────────────────────────

st.title("📈 Market Intelligence")
st.caption(
    "Grounded in pipeline data (dbt STOCK_PERFORMANCE) "
    "+ financial news (Pinecone) · Via FastAPI · LLM: Groq llama-3.3-70b-versatile"
)
st.write(" · ".join([f"`{t}`" for t in
    ["AAPL","MSFT","GOOGL","NVDA","TSLA","META","AMZN","JPM","INTC","V"]]))
st.divider()


# ── Session state ─────────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "quick_q" not in st.session_state:
    st.session_state["quick_q"] = None


# ── Chat history ──────────────────────────────────────────────────────────────

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"],
                         avatar="🧑" if msg["role"] == "user" else "🤖"):
        if msg["role"] == "assistant" and msg.get("metrics"):
            sym = msg.get("symbol", "")
            if sym and sym != "All Stocks":
                render_pipeline_metrics(msg["metrics"], sym)
        st.write(safe_str(msg.get("content"), ""))
        if msg["role"] == "assistant" and msg.get("sources"):
            render_sources(msg["sources"])


# ── Chat input ────────────────────────────────────────────────────────────────

user_input = st.chat_input("Ask anything about your tracked stocks...")

if st.session_state.get("quick_q") and not user_input:
    user_input = st.session_state["quick_q"]
    st.session_state["quick_q"] = None


# ── Process input ─────────────────────────────────────────────────────────────

if user_input and user_input.strip():
    question = user_input.strip()

    # sym defined OUTSIDE all context managers so it's accessible
    # to both the spinner block and the render call after it closes
    sym = selected_stock if selected_stock != "All Stocks" else None

    with st.chat_message("user", avatar="🧑"):
        st.write(question)

    st.session_state["messages"].append({
        "role":    "user",
        "content": question,
    })

    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("Fetching pipeline data · Searching Pinecone · Generating with Groq..."):
            try:
                result = query_intelligence(question, sym)

                if "error" in result:
                    answer  = f"⚠️ API error: {result['error']}"
                    chunks  = []
                    metrics = {}
                else:
                    answer  = result.get("answer", "No answer returned")
                    chunks  = result.get("sources", [])
                    metrics = result.get("pipeline_metrics") or {}

                    

            except Exception as e:
                metrics = {}
                chunks  = []
                answer  = f"⚠️ Pipeline error: {str(e)}"

        # Outside spinner, inside chat_message — identical pattern to old working file
        if metrics and sym and sym != "All Stocks":
            render_pipeline_metrics(metrics, sym)

        st.write(answer)
        render_sources(chunks)

    st.session_state["messages"].append({
        "role":    "assistant",
        "content": answer,
        "sources": chunks,
        "metrics": metrics,
        "symbol":  selected_stock,
    })


# ── Empty state ───────────────────────────────────────────────────────────────

if not st.session_state["messages"]:
    st.info(
        "Ask anything about your 10 tracked stocks.\n\n"
        "Answers are grounded in **quantitative pipeline signals** "
        "(SMA crossovers, BUY/SELL signals from dbt) "
        "**and** real financial news (Pinecone semantic search)."
    )
    c1, c2, c3 = st.columns(3)
    if c1.button("AAPL outlook?", key="ex1"):
        st.session_state["quick_q"] = "What is the outlook for AAPL?"
        st.rerun()
    if c2.button("NVDA supply chain?", key="ex2"):
        st.session_state["quick_q"] = "Any supply chain risks for NVDA?"
        st.rerun()
    if c3.button("Market sentiment?", key="ex3"):
        st.session_state["quick_q"] = "What is the overall market sentiment?"
        st.rerun()