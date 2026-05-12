"""
Stocks router — historical data, realtime windows, dbt signals.

Endpoints:
  GET /api/v1/stocks/historical?symbol=AAPL&days=30
  GET /api/v1/stocks/realtime?symbol=AAPL
  GET /api/v1/stocks/signals?symbol=AAPL
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.models.responses import HistoricalResponse, RealtimeResponse, SignalsResponse
import api.services.snowflake as sf_svc
from api.config import TRACKED_STOCKS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stocks", tags=["stocks"])

VALID_SYMBOLS = set(TRACKED_STOCKS)


def _validate_symbol(symbol: Optional[str]) -> Optional[str]:
    """Validate symbol is one of the 10 tracked stocks."""
    if symbol is None:
        return None
    sym = symbol.upper()
    if sym not in VALID_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"Symbol '{sym}' not tracked. Valid: {sorted(VALID_SYMBOLS)}"
        )
    return sym


def _validate_symbol_required(symbol: str) -> str:
    """Same as _validate_symbol but for required query params (always returns str)."""
    sym = symbol.upper()
    if sym not in VALID_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"Symbol '{sym}' not tracked. Valid: {sorted(VALID_SYMBOLS)}"
        )
    return sym


@router.get(
    "/historical",
    response_model=HistoricalResponse,
    summary="Get historical OHLCV data",
    description="Returns historical price data with SMA-5, SMA-20, and daily return"
)
async def get_historical(
    symbol: str  = Query(..., description="Stock symbol e.g. AAPL"),
    days:   int  = Query(30, ge=1, le=365, description="Number of days to return")
):
    sym = _validate_symbol_required(symbol)
    try:
        return sf_svc.get_historical_data(sym, days)
    except Exception as e:
        logger.error(f"historical endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/realtime",
    response_model=RealtimeResponse,
    summary="Get latest realtime windowed metrics",
    description="Returns latest 15-min and 1-hour moving averages from streaming pipeline"
)
async def get_realtime(
    symbol: Optional[str] = Query(None, description="Stock symbol, omit for all stocks")
):
    sym = _validate_symbol(symbol) if symbol else None
    try:
        return sf_svc.get_realtime_data(sym)
    except Exception as e:
        logger.error(f"realtime endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/signals",
    response_model=SignalsResponse,
    summary="Get dbt BUY/SELL signals",
    description="Returns trading signals from dbt STOCK_PERFORMANCE mart"
)
async def get_signals(
    symbol: Optional[str] = Query(None, description="Stock symbol, omit for all stocks")
):
    sym = _validate_symbol(symbol) if symbol else None
    try:
        return sf_svc.get_signals(sym)
    except Exception as e:
        logger.error(f"signals endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))