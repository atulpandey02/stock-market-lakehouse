"""
Stock Market Intelligence — FastAPI service
Entry point — registers all routers and middleware.

Start with: uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
Docs at:    http://localhost:8000/docs
"""

import logging
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.config import API_VERSION
from api.routers import health, stocks, pipeline, intelligence, sql

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title       = "Stock Market Intelligence API",
    description = """
API layer for the Stock Market Lakehouse platform.

Exposes:
- Historical OHLCV data from Snowflake
- Realtime windowed metrics from streaming pipeline
- BUY/SELL signals from dbt STOCK_PERFORMANCE mart
- RAG market intelligence (Pinecone + Groq)
- Pipeline KPIs
- SQL Explorer (read-only)
    """,
    version     = "2.0.0",
    docs_url    = "/docs",     # Swagger UI
    redoc_url   = "/redoc",    # ReDoc UI
)

# ── CORS ──────────────────────────────────────────────────────────────────────
# Allows Streamlit (running on port 8501) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],   # tighten this in production
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# ── Request logging middleware ────────────────────────────────────────────────
# Logs every request with timing — this is your observability layer
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start  = time.time()
    method = request.method
    path   = request.url.path

    response = await call_next(request)

    duration = (time.time() - start) * 1000
    logger.info(
        f"{method} {path} → {response.status_code} "
        f"({duration:.1f}ms)"
    )
    return response

# ── Routers ───────────────────────────────────────────────────────────────────
# All endpoints live under /api/v1/
prefix = f"/api/{API_VERSION}"

app.include_router(health.router,       prefix=prefix)
app.include_router(stocks.router,       prefix=prefix)
app.include_router(pipeline.router,     prefix=prefix)
app.include_router(intelligence.router, prefix=prefix)
app.include_router(sql.router,          prefix=prefix)

# ── Root ──────────────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def root():
    return {
        "service":   "Stock Market Intelligence API",
        "version":   "2.0.0",
        "docs":      "/docs",
        "health":    f"/api/{API_VERSION}/health",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ── Global exception handler ──────────────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )