import os

from dotenv import load_dotenv


# Load environment from a local .env if present (no-op in Docker if not mounted)
load_dotenv()


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default


# ── API ────────────────────────────────────────────────────────────────────────

API_VERSION = os.getenv("API_VERSION", "v1")

TRACKED_STOCKS = [
    s.strip().upper()
    for s in os.getenv(
        "TRACKED_STOCKS",
        "AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA,META,JPM,V,UNH",
    ).split(",")
    if s.strip()
]


# ── Snowflake ──────────────────────────────────────────────────────────────────

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")

SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

# These map to your two logical DBs (batch & stream) used in `api/services/snowflake.py`
SNOWFLAKE_DB_BATCH = os.getenv("SNOWFLAKE_DATABASE", os.getenv("SNOWFLAKE_DB_BATCH", "STOCKMARKETBATCH"))
SNOWFLAKE_DB_STREAM = os.getenv(
    "SNOWFLAKE_STREAM_DATABASE",
    os.getenv("SNOWFLAKE_DB_STREAM", "STOCKMARKETSTREAM"),
)


# ── Pinecone / Embeddings ──────────────────────────────────────────────────────

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY", "")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "")

# Must match ingestion embedding model used by your RAG pipeline
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")


# ── Groq ───────────────────────────────────────────────────────────────────────

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1/chat/completions")
GROQ_RATE_LIMIT_PER_MINUTE = _get_int("GROQ_RATE_LIMIT_PER_MINUTE", 30)


# ── Caching TTLs (seconds) ─────────────────────────────────────────────────────

CACHE_TTL_HISTORICAL = _get_int("CACHE_TTL_HISTORICAL", 300)  # 5m
CACHE_TTL_REALTIME = _get_int("CACHE_TTL_REALTIME", 30)      # 30s
CACHE_TTL_SIGNALS = _get_int("CACHE_TTL_SIGNALS", 300)       # 5m
CACHE_TTL_KPIS = _get_int("CACHE_TTL_KPIS", 60)              # 1m

