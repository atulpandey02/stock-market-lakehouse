"""
Snowflake service — manages ONE connection reused across all requests.

Key design decisions:
1. Single connection opened at app startup (not per request)
   → saves 2-3 seconds per query
2. All queries return Pydantic models, never raw rows
   → Streamlit always gets a guaranteed shape
3. Cache layer wraps every method
   → avoids hammering Snowflake on every dashboard refresh
"""

import logging
from typing import Any, List, Optional, Tuple
from datetime import date, datetime

import snowflake.connector

from api.config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
    SNOWFLAKE_DB_BATCH, SNOWFLAKE_DB_STREAM,
    CACHE_TTL_HISTORICAL, CACHE_TTL_REALTIME,
    CACHE_TTL_SIGNALS, CACHE_TTL_KPIS
)
from api.models.responses import (
    HistoricalBar, HistoricalResponse,
    RealtimeWindow, RealtimeResponse,
    SignalBar, SignalsResponse,
    PipelineKPIs, SQLResponse
)
from api.services.cache import cache

logger = logging.getLogger(__name__)


def _get_connection(database: str):
    """
    Open a Snowflake connection.
    Called per-request but kept lightweight via Snowflake's
    internal connection pooling.
    """
    return snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        warehouse = SNOWFLAKE_WAREHOUSE,
        role      = SNOWFLAKE_ROLE,
        database  = database,
        schema    = "PUBLIC",
    )


def _run_query(sql: str, database: str) -> Tuple[List[Any], List[str]]:
    """
    Execute SQL and return (rows, columns).
    Context manager ensures connection is always closed.
    """
    conn = _get_connection(database)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
        return rows, cols
    finally:
        conn.close()


# ── Historical data ───────────────────────────────────────────────────────────

def get_historical_data(symbol: str, days: int = 30) -> HistoricalResponse:
    """
    Fetch OHLCV + SMA data from HISTORICAL_STOCK.
    Cached for 5 minutes — historical data doesn't change often.
    """
    cache_key = f"historical:{symbol}:{days}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    sql = f"""
        SELECT
            DATE, SYMBOL,
            ROUND(OPEN_PRICE,       2) AS OPEN_PRICE,
            ROUND(HIGH_PRICE,       2) AS HIGH_PRICE,
            ROUND(LOW_PRICE,        2) AS LOW_PRICE,
            ROUND(CLOSE_PRICE,      2) AS CLOSE_PRICE,
            VOLUME,
            ROUND(DAILY_RETURN_PCT, 2) AS DAILY_RETURN_PCT,
            ROUND(SMA_5,            2) AS SMA_5,
            ROUND(SMA_20,           2) AS SMA_20
        FROM HISTORICAL_STOCK
        WHERE SYMBOL = '{symbol.upper()}'
        ORDER BY DATE DESC
        LIMIT {days}
    """

    try:
        rows, cols = _run_query(sql, SNOWFLAKE_DB_BATCH)
        bars = []
        for row in rows:
            d = dict(zip([c.lower() for c in cols], row))
            bars.append(HistoricalBar(**d))

        result = HistoricalResponse(
            symbol=symbol.upper(),
            days=days,
            count=len(bars),
            data=bars
        )
        cache.set(cache_key, result, CACHE_TTL_HISTORICAL)
        return result

    except Exception as e:
        logger.error(f"get_historical_data error: {e}")
        raise


# ── Realtime data ─────────────────────────────────────────────────────────────

def get_realtime_data(symbol: Optional[str] = None) -> RealtimeResponse:
    """
    Fetch latest windowed metrics from REALTIME_STOCK.
    Cached for 30 seconds — realtime data updates frequently.
    """
    cache_key = f"realtime:{symbol or 'all'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    where = f"WHERE SYMBOL = '{symbol.upper()}'" if symbol else ""
    sql = f"""
        SELECT
            SYMBOL, WINDOW_START,
            ROUND(MA_15M,         2) AS MA_15M,
            ROUND(MA_1H,          2) AS MA_1H,
            ROUND(VOLATILITY_15M, 4) AS VOLATILITY_15M,
            VOLUME_SUM_1H
        FROM REALTIME_STOCK
        {where}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY SYMBOL ORDER BY WINDOW_START DESC
        ) = 1
        ORDER BY SYMBOL
    """

    try:
        rows, cols = _run_query(sql, SNOWFLAKE_DB_STREAM)
        windows = []
        for row in rows:
            d = dict(zip([c.lower() for c in cols], row))
            windows.append(RealtimeWindow(**d))

        result = RealtimeResponse(
            symbol=symbol or "ALL",
            count=len(windows),
            data=windows
        )
        cache.set(cache_key, result, CACHE_TTL_REALTIME)
        return result

    except Exception as e:
        logger.error(f"get_realtime_data error: {e}")
        raise


# ── dbt signals ───────────────────────────────────────────────────────────────

def get_signals(symbol: Optional[str] = None) -> SignalsResponse:
    """
    Fetch BUY/SELL signals from dbt STOCK_PERFORMANCE mart.
    Cached for 5 minutes.
    """
    cache_key = f"signals:{symbol or 'all'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    where = f"WHERE SYMBOL = '{symbol.upper()}'" if symbol else ""
    sql = f"""
        SELECT
            SYMBOL, TRADE_DATE,
            ROUND(CLOSE_PRICE,      2) AS CLOSE_PRICE,
            ROUND(DAILY_RETURN_PCT, 2) AS DAILY_RETURN_PCT,
            ROUND(SMA_5,            2) AS SMA_5,
            ROUND(SMA_20,           2) AS SMA_20,
            SMA_SIGNAL,
            OVERALL_SIGNAL
        FROM STOCK_PERFORMANCE
        {where}
        ORDER BY SYMBOL
    """

    try:
        rows, cols = _run_query(sql, SNOWFLAKE_DB_BATCH)
        signals = []
        for row in rows:
            d = dict(zip([c.lower() for c in cols], row))
            signals.append(SignalBar(**d))

        result = SignalsResponse(
            symbol=symbol or "ALL",
            data=signals
        )
        cache.set(cache_key, result, CACHE_TTL_SIGNALS)
        return result

    except Exception as e:
        logger.error(f"get_signals error: {e}")
        raise


# ── Pipeline KPIs ─────────────────────────────────────────────────────────────

def get_pipeline_kpis() -> PipelineKPIs:
    """
    Aggregate KPIs across both databases.
    Cached for 1 minute.
    """
    cache_key = "pipeline:kpis"
    cached = cache.get(cache_key)
    if cached:
        return cached

    try:
        # Batch DB queries
        rows_total, _ = _run_query(
            "SELECT COUNT(*) AS CNT FROM HISTORICAL_STOCK",
            SNOWFLAKE_DB_BATCH
        )
        rows_symbols, _ = _run_query(
            "SELECT COUNT(DISTINCT SYMBOL) AS CNT FROM HISTORICAL_STOCK",
            SNOWFLAKE_DB_BATCH
        )
        rows_date, _ = _run_query(
            "SELECT MAX(DATE) AS LATEST FROM HISTORICAL_STOCK",
            SNOWFLAKE_DB_BATCH
        )
        rows_pos, _ = _run_query(
            """SELECT ROUND(
                SUM(CASE WHEN IS_POSITIVE_DAY THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
               ) AS PCT FROM HISTORICAL_STOCK""",
            SNOWFLAKE_DB_BATCH
        )

        # Stream DB query
        rows_rt, _ = _run_query(
            "SELECT COUNT(*) AS CNT FROM REALTIME_STOCK",
            SNOWFLAKE_DB_STREAM
        )

        result = PipelineKPIs(
            historical_rows   = int(rows_total[0][0] or 0),
            symbol_count      = int(rows_symbols[0][0] or 0),
            latest_batch_date = str(rows_date[0][0]) if rows_date[0][0] else None,
            realtime_windows  = int(rows_rt[0][0] or 0),
            positive_day_pct  = float(rows_pos[0][0] or 0),
        )
        cache.set(cache_key, result, CACHE_TTL_KPIS)
        return result

    except Exception as e:
        logger.error(f"get_pipeline_kpis error: {e}")
        raise


# ── SQL Explorer ──────────────────────────────────────────────────────────────

def run_raw_query(sql: str, database: str) -> SQLResponse:
    """
    Run arbitrary SQL — used by SQL Explorer page.
    NOT cached — user expects fresh results.
    Basic safety: block destructive statements.
    """
    # Block destructive SQL — read-only API
    blocked = ["drop ", "delete ", "truncate ", "insert ", "update ", "alter "]
    if any(sql.lower().strip().startswith(b) for b in blocked):
        raise ValueError("Only SELECT statements are allowed")

    try:
        rows, cols = _run_query(sql, database)
        # Convert rows to list of dicts for JSON serialization
        result_rows = [
            dict(zip(cols, [str(v) if v is not None else None for v in row]))
            for row in rows
        ]
        return SQLResponse(
            row_count    = len(rows),
            column_count = len(cols),
            columns      = cols,
            rows         = result_rows,
            database     = database
        )

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"run_raw_query error: {e}")
        raise