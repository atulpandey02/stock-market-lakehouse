"""
Pydantic response models — defines the shape of every API response.
Streamlit only sees these shapes, never raw Snowflake rows or Pinecone dicts.
"""

from typing import List, Optional, Any, Dict
from datetime import date, datetime
from pydantic import BaseModel


# ── Health ────────────────────────────────────────────────────────────────────

class ServiceStatus(BaseModel):
    name:    str
    status:  str       # "ok" | "error"
    message: str = ""

class HealthResponse(BaseModel):
    status:   str                  # "healthy" | "degraded"
    services: List[ServiceStatus]
    timestamp: datetime


# ── Stocks ────────────────────────────────────────────────────────────────────

class HistoricalBar(BaseModel):
    date:             date
    symbol:           str
    open_price:       Optional[float]
    high_price:       Optional[float]
    low_price:        Optional[float]
    close_price:      Optional[float]
    volume:           Optional[int]
    daily_return_pct: Optional[float]
    sma_5:            Optional[float]
    sma_20:           Optional[float]

class HistoricalResponse(BaseModel):
    symbol: str
    days:   int
    count:  int
    data:   List[HistoricalBar]


class RealtimeWindow(BaseModel):
    symbol:         str
    window_start:   Optional[datetime]
    ma_15m:         Optional[float]
    ma_1h:          Optional[float]
    volatility_15m: Optional[float]
    volume_sum_1h:  Optional[int]

class RealtimeResponse(BaseModel):
    symbol: str
    count:  int
    data:   List[RealtimeWindow]


class SignalBar(BaseModel):
    symbol:           str
    trade_date:       Optional[date]
    close_price:      Optional[float]
    daily_return_pct: Optional[float]
    sma_5:            Optional[float]
    sma_20:           Optional[float]
    sma_signal:       Optional[str]
    overall_signal:   Optional[str]

class SignalsResponse(BaseModel):
    symbol: str
    data:   List[SignalBar]


# ── Pipeline KPIs ─────────────────────────────────────────────────────────────

class PipelineKPIs(BaseModel):
    historical_rows:    int
    symbol_count:       int
    latest_batch_date:  Optional[str]
    realtime_windows:   int
    positive_day_pct:   Optional[float]
    dbt_tests_passing:  str = "27 / 27"


# ── Intelligence (RAG) ────────────────────────────────────────────────────────

class NewsSource(BaseModel):
    title:        str
    summary:      str
    symbol:       str
    source:       str
    url:          str
    published_at: str
    sentiment:    str
    score:        float

class IntelligenceResponse(BaseModel):
    question:      str
    symbol_filter: Optional[str]
    answer:        str
    sources:       List[NewsSource]
    total_sources: int
    pipeline_metrics: Optional[dict] = None


# ── SQL Explorer ──────────────────────────────────────────────────────────────

class SQLResponse(BaseModel):
    row_count:    int
    column_count: int
    columns:      List[str]
    rows:         List[Dict[str, Any]]
    database:     str


# ── Generic error ─────────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    error:   str
    detail:  str = ""