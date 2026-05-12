"""
Stock Market Intelligence — Streamlit Chat Interface
Powered by: Finnhub News → Pinecone Vector Search → Groq Llama3

Run: streamlit run market_intelligence_app.py
"""

import os
import sys
import warnings
import logging
import requests
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# ── Suppress warnings ─────────────────────────────────────────────────────────
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# ── Path setup ────────────────────────────────────────────────────────────────
_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _this_dir)

# ── Load .env (override=True beats stale shell exports) ───────────────────────
_dotenv_path = find_dotenv(usecwd=True, raise_error_if_not_found=False)
if _dotenv_path:
    load_dotenv(_dotenv_path, override=True)
else:
    for _p in [
        Path(_this_dir) / ".env",
        Path(_this_dir).parent / ".env",
        Path(_this_dir).parent.parent / ".env",
        Path(_this_dir).parent.parent.parent / ".env",
    ]:
        if _p.exists():
            load_dotenv(_p, override=True)

# ── Page Config — MUST be first Streamlit call ────────────────────────────────
st.set_page_config(
    page_title="Stock Market Intelligence",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');
:root {
    --bg:      #0a0e1a;
    --surface: #111827;
    --border:  #1e293b;
    --accent:  #00d4ff;
    --accent2: #7c3aed;
    --green:   #10b981;
    --red:     #ef4444;
    --yellow:  #f59e0b;
    --text:    #e2e8f0;
    --muted:   #64748b;
}
html, body, [class*="css"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
}
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stChatInput"] textarea {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
}
[data-testid="stChatMessage"] {
    background: transparent !important;
}
.header-title {
    font-family: 'Space Mono', monospace;
    font-size: 28px; font-weight: 700;
    color: var(--accent); letter-spacing: -0.5px;
    margin: 0; padding: 24px 0 4px 0;
}
.header-sub {
    font-size: 13px; color: var(--muted);
    margin: 0 0 24px 0; font-weight: 300;
    border-bottom: 1px solid var(--border);
    padding-bottom: 16px;
}
.ticker-bar {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 10px 16px; display: flex;
    gap: 20px; font-family: 'Space Mono', monospace;
    font-size: 12px; margin-bottom: 20px; overflow-x: auto; flex-wrap: wrap;
}
.ticker-sym  { color: var(--accent); font-weight: 700; }
.ticker-item { color: var(--muted); }
.source-card {
    background: #0d1626; border: 1px solid var(--border);
    border-radius: 8px; padding: 10px 14px; margin: 5px 0; font-size: 13px;
}
.source-title { color: var(--text); font-weight: 500; margin-bottom: 3px; }
.source-meta  { color: var(--muted); font-size: 11px; }
.badge {
    display: inline-block; padding: 2px 8px; border-radius: 20px;
    font-size: 10px; font-family: 'Space Mono', monospace;
    font-weight: 700; letter-spacing: 1px; margin-left: 6px;
}
.badge-pos { background:#052e16; color:var(--green); border:1px solid var(--green); }
.badge-neg { background:#2d0a0a; color:var(--red);   border:1px solid var(--red);   }
.badge-neu { background:#1c1205; color:var(--yellow);border:1px solid var(--yellow);}
.score-pill {
    display: inline-block; background: #0f172a;
    border: 1px solid var(--border); border-radius: 12px;
    padding: 1px 8px; font-family: 'Space Mono', monospace;
    font-size: 10px; color: var(--accent); margin-left: 6px;
}
.divider { height:1px; background:var(--border); margin:16px 0; }
.stat-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px; text-align: center; margin-bottom: 8px;
}
.stat-value {
    font-family: 'Space Mono', monospace; font-size: 28px;
    font-weight: 700; color: var(--accent); display: block;
}
.stat-label { font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:1px; }
[data-testid="stSidebar"] .stButton > button {
    background: var(--surface) !important; color: var(--text) !important;
    border: 1px solid var(--border) !important;
    font-size: 11px !important; text-align: left !important;
    width: 100% !important;
}
</style>
""", unsafe_allow_html=True)


# ── Backend Functions ─────────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading embedding model...")
def load_embedding_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def retrieve_from_pinecone(question: str, symbol: str | None = None, top_k: int = 5):
    from pinecone import Pinecone
    model     = load_embedding_model()
    query_vec = model.encode([question])[0].tolist()
    pc        = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    index     = pc.Index(os.getenv("PINECONE_INDEX_NAME", "stock-market-rag"))
    params    = {"vector": query_vec, "top_k": top_k, "include_metadata": True}
    if symbol and symbol != "All Stocks":
        params["filter"] = {"symbol": symbol.upper()}
    results = index.query(**params)
    matches = results.get("matches", [])
    seen, unique = set(), []
    for m in matches:
        url = m["metadata"].get("url", "")
        if url not in seen:
            seen.add(url)
            unique.append({
                "title":        m["metadata"].get("title")        or "N/A",
                "summary":      m["metadata"].get("summary")       or "",
                "symbol":       m["metadata"].get("symbol")        or "",
                "source":       m["metadata"].get("source")        or "",
                "url":          url,
                "published_at": m["metadata"].get("published_at") or "",
                "sentiment":    m["metadata"].get("sentiment")     or "Neutral",
                "score":        round(m.get("score") or 0, 3),
            })
    return unique


def generate_with_groq(question: str, chunks: list) -> str:
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        return "⚠️ GROQ_API_KEY is missing — check your .env file."
    if not chunks:
        return "⚠️ No relevant articles found in Pinecone for this query."
    context = "\n\n".join([
        f"[{c['symbol']}] {c['title']}\n"
        f"Published: {c['published_at'][:10]} | Source: {c['source']}\n"
        f"Sentiment: {c['sentiment']}\n"
        f"Summary: {c['summary']}"
        for c in chunks
    ])
    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {groq_key}",
                "Content-Type":  "application/json",
            },
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a professional financial market analyst. "
                            "Answer questions using ONLY the provided news articles. "
                            "Be concise (3-5 sentences). Cite sources where relevant. "
                            "Do not use external knowledge."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"News Articles:\n{context}\n\n"
                            f"Question: {question}\n\nAnalysis:"
                        ),
                    },
                ],
                "temperature": 0.1,
                "max_tokens":  500,
            },
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"].strip()
        else:
            return f"⚠️ Groq API error {response.status_code}: {response.text[:300]}"
    except requests.exceptions.Timeout:
        return "⚠️ Groq API timed out after 30s."
    except Exception as e:
        return f"⚠️ Unexpected error: {str(e)}"


def get_index_stats():
    try:
        from pinecone import Pinecone
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "stock-market-rag"))
        stats = index.describe_index_stats()
        return {"total_vectors": stats.total_vector_count,
                "dimension": stats.dimension, "status": "Connected ✓"}
    except Exception as e:
        return {"total_vectors": 0, "dimension": 384, "status": f"Error: {e}"}


def sentiment_badge(sentiment: str) -> str:
    s = (sentiment or "Neutral").lower()
    if "positive" in s: return '<span class="badge badge-pos">POSITIVE</span>'
    if "negative" in s: return '<span class="badge badge-neg">NEGATIVE</span>'
    return '<span class="badge badge-neu">NEUTRAL</span>'


def render_sources(sources: list):
    if not sources:
        return
    st.markdown(
        f"<div style='font-family:Space Mono,monospace;font-size:10px;"
        f"color:#64748b;letter-spacing:2px;margin:12px 0 6px 0;'>"
        f"SOURCES ({len(sources)})</div>",
        unsafe_allow_html=True,
    )
    for i, src in enumerate(sources, 1):
        badge = sentiment_badge(src.get("sentiment", "Neutral"))
        score = src.get("score", 0)
        pub   = (src.get("published_at") or "")[:10]
        st.markdown(f"""
            <div class='source-card'>
                <div class='source-title'>
                    [{i}] {src.get('title', 'N/A')}
                    {badge}
                    <span class='score-pill'>{score:.3f}</span>
                </div>
                <div class='source-meta'>
                    <b style='color:#94a3b8;'>{src.get('symbol','')}</b> ·
                    {src.get('source','')} · {pub} ·
                    <a href="{src.get('url','#')}" target="_blank"
                       style="color:#00d4ff;text-decoration:none;">View →</a>
                </div>
            </div>
        """, unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style="font-family:'Space Mono',monospace;font-size:11px;
                    color:#00d4ff;letter-spacing:2px;margin-bottom:20px;">
            ◈ MARKET INTELLIGENCE
        </div>
    """, unsafe_allow_html=True)

    st.markdown("**Filter by Stock**")
    stocks = ["All Stocks","AAPL","MSFT","GOOGL","AMZN",
              "META","TSLA","NVDA","INTC","JPM","V"]
    selected_stock = st.selectbox("Stock filter", stocks, label_visibility="collapsed")

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown("**Quick Questions**")
    ticker = selected_stock if selected_stock != "All Stocks" else "AAPL"

    # ── FIX: quick questions now write to session_state key ──────────────────
    # st.chat_input reads from this key on the next run
    for q in [
        "What is the current market sentiment?",
        f"What is the outlook for {ticker}?",
        "Any supply chain concerns for tech stocks?",
        "What are analysts saying about earnings?",
        "Any regulatory risks to watch?",
    ]:
        if st.button(q, key=f"qq_{q}", use_container_width=True):
            st.session_state["quick_q"] = q
            st.rerun()

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown("**Pipeline**")
    st.markdown("""
        <div style='font-size:11px;color:#64748b;line-height:1.8;'>
            <div>📰 <b style='color:#e2e8f0;'>News</b> Finnhub API</div>
            <div>🔢 <b style='color:#e2e8f0;'>Embeddings</b> all-MiniLM-L6-v2</div>
            <div>🗄️ <b style='color:#e2e8f0;'>Vector DB</b> Pinecone</div>
            <div>🧠 <b style='color:#e2e8f0;'>LLM</b> Groq Llama3-8b</div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown("**Pinecone Index**")
    if st.button("Check Status", use_container_width=True, key="check_status"):
        with st.spinner("Checking..."):
            stats = get_index_stats()
        st.markdown(f"""
            <div class='stat-card'>
                <span class='stat-value'>{stats['total_vectors']:,}</span>
                <div class='stat-label'>Vectors Stored</div>
            </div>
            <div style='font-size:11px;color:#64748b;margin-top:4px;'>
                Dim: {stats['dimension']} · {stats['status']}
            </div>
        """, unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    if st.button("Clear Chat", use_container_width=True, key="clear_chat"):
        st.session_state["messages"] = []
        st.rerun()


# ── Main header ───────────────────────────────────────────────────────────────
st.markdown("<h1 class='header-title'>📈 Market Intelligence</h1>", unsafe_allow_html=True)
st.markdown("""
    <p class='header-sub'>RAG-powered financial analysis · Finnhub News · Pinecone · Groq Llama3</p>
""", unsafe_allow_html=True)

st.markdown("""
    <div class='ticker-bar'>
        <span class='ticker-item'><span class='ticker-sym'>AAPL</span> Apple</span>
        <span class='ticker-item'><span class='ticker-sym'>MSFT</span> Microsoft</span>
        <span class='ticker-item'><span class='ticker-sym'>GOOGL</span> Alphabet</span>
        <span class='ticker-item'><span class='ticker-sym'>NVDA</span> NVIDIA</span>
        <span class='ticker-item'><span class='ticker-sym'>TSLA</span> Tesla</span>
        <span class='ticker-item'><span class='ticker-sym'>META</span> Meta</span>
        <span class='ticker-item'><span class='ticker-sym'>AMZN</span> Amazon</span>
        <span class='ticker-item'><span class='ticker-sym'>JPM</span> JPMorgan</span>
    </div>
""", unsafe_allow_html=True)

# ── Session state init ────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "quick_q" not in st.session_state:
    st.session_state["quick_q"] = None

# ── Render existing chat history ──────────────────────────────────────────────
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"], avatar="🧑" if msg["role"] == "user" else "🤖"):
        st.markdown(msg["content"])
        if msg["role"] == "assistant" and msg.get("sources"):
            render_sources(msg["sources"])

# ── Chat input — THIS IS THE FIX ─────────────────────────────────────────────
# st.chat_input does NOT have the clear_on_submit bug.
# It returns the submitted string directly — no form gymnastics needed.
user_input = st.chat_input(
    "Ask anything about your tracked stocks...",
    key="chat_input",
)

# Handle quick question buttons from sidebar
if st.session_state["quick_q"] and not user_input:
    user_input = st.session_state["quick_q"]
    st.session_state["quick_q"] = None

# ── Process input ─────────────────────────────────────────────────────────────
if user_input and user_input.strip():
    question = user_input.strip()

    # Show user message immediately
    with st.chat_message("user", avatar="🧑"):
        st.markdown(question)

    # Save user message
    st.session_state["messages"].append({
        "role": "user",
        "content": question,
    })

    # Generate response
    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("🔍 Searching Pinecone · Generating with Groq Llama3..."):
            try:
                sym    = selected_stock if selected_stock != "All Stocks" else None
                chunks = retrieve_from_pinecone(question, symbol=sym, top_k=5)
                answer = generate_with_groq(question, chunks)
            except Exception as e:
                chunks = []
                answer = f"⚠️ Pipeline error: {str(e)}"

        st.markdown(answer)
        if chunks:
            render_sources(chunks)

    # Save assistant message
    st.session_state["messages"].append({
        "role":    "assistant",
        "content": answer,
        "sources": chunks,
    })

# ── Empty state ───────────────────────────────────────────────────────────────
if not st.session_state["messages"]:
    st.markdown("""
        <div style='text-align:center;padding:60px 0;'>
            <div style='font-size:52px;margin-bottom:16px;'>◈</div>
            <div style='font-family:Space Mono,monospace;font-size:14px;
                        letter-spacing:2px;color:#334155;margin-bottom:8px;'>
                AWAITING QUERY
            </div>
            <div style='font-size:13px;color:#334155;
                        max-width:440px;margin:0 auto 32px auto;'>
                Ask anything about your 10 tracked stocks.
                AI-powered analysis grounded in real financial news.
            </div>
            <div style='display:flex;justify-content:center;gap:10px;flex-wrap:wrap;'>
                <div style='background:#111827;border:1px solid #1e293b;
                            border-radius:20px;padding:6px 14px;
                            font-size:12px;color:#64748b;'>
                    "What's the AAPL outlook?"
                </div>
                <div style='background:#111827;border:1px solid #1e293b;
                            border-radius:20px;padding:6px 14px;
                            font-size:12px;color:#64748b;'>
                    "Any NVDA supply chain risks?"
                </div>
                <div style='background:#111827;border:1px solid #1e293b;
                            border-radius:20px;padding:6px 14px;
                            font-size:12px;color:#64748b;'>
                    "Overall market sentiment?"
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)