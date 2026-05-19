"""
Stock Market Intelligence Platform
Multi-page Streamlit app — entry point

Run: streamlit run app.py
"""
import streamlit as st

st.set_page_config(
    page_title = "Stock Market Intelligence",
    page_icon  = "📈",
    layout     = "wide",
    initial_sidebar_state = "expanded"
)

st.title("📈 Stock Market Intelligence Platform")
st.caption("Kafka · Spark · Airflow · Snowflake · dbt · Pinecone · Groq Llama3.3")

st.divider()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.info(
        "**🤖 Market Intelligence**\n\n"
        "Ask questions about stocks using RAG-powered analysis "
        "from real financial news via Groq Llama3.3"
    )
    st.page_link("pages/1_Market_Intelligence_App.py", label="Open Market Intelligence →", use_container_width=True)

with col2:
    st.success(
        "**📊 Pipeline Dashboard**\n\n"
        "Real-time pipeline monitoring, buy/sell signals "
        "from dbt models, and live data quality checks"
    )
    st.page_link("pages/2_Pipeline_Dashboard.py", label="Open Pipeline Dashboard →", use_container_width=True)

with col3:
    st.warning(
        "**🔍 SQL Explorer**\n\n"
        "Ask questions in plain English or write raw SQL "
        "directly against Snowflake — powered by NL2SQL"
    )
    st.page_link("pages/3_Sql_Explorer.py", label="Open SQL Explorer →", use_container_width=True)

with col4:
    st.error(
        "**🔬 RAG Monitor**\n\n"
        "Medallion architecture observability — semantic "
        "similarity scores, latency, and pipeline health"
    )
    st.page_link("pages/4_Rag_Monitor.py", label="Open RAG Monitor →", use_container_width=True)

st.divider()

# ── Tech stack badges ─────────────────────────────────────────────────────────
st.caption("**Tech Stack**")
cols = st.columns(9)
techs = [
    ("⚡", "Kafka"),
    ("🔥", "Spark"),
    ("🌊", "Airflow"),
    ("❄️", "Snowflake"),
    ("🔷", "dbt"),
    ("🗂️", "Iceberg"),
    ("📌", "Pinecone"),
    ("🤖", "Groq"),
    ("🚀", "FastAPI"),
]
for i, (icon, name) in enumerate(techs):
    cols[i].metric(icon, name)