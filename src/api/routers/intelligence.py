"""
Intelligence router — full RAG pipeline in one endpoint.

POST /api/v1/intelligence/query

This endpoint orchestrates:
1. Snowflake → get dbt metrics for context
2. Pinecone → semantic search for relevant news
3. Groq     → generate grounded answer

Previously these 3 calls were scattered across Streamlit pages.
Now they're in one place, testable, cacheable, and observable.
"""

import logging
import time 
from fastapi import APIRouter, HTTPException

from api.models.requests import IntelligenceRequest
from api.models.responses import IntelligenceResponse
import api.services.snowflake as sf_svc
import api.services.pinecone_svc as pc_svc
import api.services.groq_svc as groq_svc
import api.services.lagging_svc as log_svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/intelligence", tags=["intelligence"])


def _format_metrics_for_prompt(metrics_data: dict, symbol: str) -> str:
    """Format dbt metrics into a string for the Groq prompt."""
    if not metrics_data:
        return ""

    data = metrics_data.get("data", [])
    if not data:
        return ""

    m = data[0]  # Latest record

    return f"""
=== QUANTITATIVE PIPELINE DATA for {symbol.upper()} ===
Close Price    : ${m.get('close_price', 'N/A')}
SMA-5          : ${m.get('sma_5', 'N/A')}
SMA-20         : ${m.get('sma_20', 'N/A')}
Daily Return   : {m.get('daily_return_pct', 'N/A')}%
SMA Signal     : {m.get('sma_signal', 'N/A')}
Overall Signal : {m.get('overall_signal', 'N/A')}
Data source    : dbt STOCK_PERFORMANCE mart (Snowflake)
=== END PIPELINE DATA ===
"""


@router.post(
    "/query",
    response_model=IntelligenceResponse,
    summary="RAG market intelligence query",
    description="Ask a question about stocks — grounded in pipeline data and news"
)
async def query_intelligence(request: IntelligenceRequest):
    
    start_time = time.time()  # ← START TIMER
    
    try:
        symbol = request.symbol.upper() if request.symbol else None

        # Step 1: Get dbt metrics from Snowflake (augmentation)
        pipeline_metrics_str  = ""
        pipeline_metrics_dict = {}
        if symbol:
            try:
                signals = sf_svc.get_signals(symbol)
                if signals.data:
                    latest = signals.data[0]
                    pipeline_metrics_dict = latest.model_dump()
                    pipeline_metrics_str  = _format_metrics_for_prompt(
                        {"data": [latest.model_dump()]}, symbol
                    )
            except Exception as e:
                logger.warning(f"Could not fetch dbt metrics: {e}")

        # Step 2: Semantic search Pinecone (retrieval)
        sources = pc_svc.search(
            question=request.question,
            symbol=symbol,
            top_k=request.top_k
        )

        # Step 3: Generate answer with Groq (generation)
        answer = groq_svc.generate(
            question=request.question,
            sources=sources,
            pipeline_metrics=pipeline_metrics_str
        )

        # Step 4: Calculate latency and log to BRONZE layer
        latency_ms = int((time.time() - start_time) * 1000)
        log_svc.log_intelligence_event(
            question   = request.question,
            symbol     = symbol,
            sources    = [s.model_dump() for s in sources],
            answer     = answer,
            latency_ms = latency_ms,
            top_k      = request.top_k,
        )

        return IntelligenceResponse(
            question         = request.question,
            symbol_filter    = symbol,
            answer           = answer,
            sources          = sources,
            total_sources    = len(sources),
            pipeline_metrics = pipeline_metrics_dict,
        )

    except Exception as e:
        # Log errors too — important for monitoring
        latency_ms = int((time.time() - start_time) * 1000)
        log_svc.log_intelligence_event(
            question   = request.question,
            symbol     = symbol if 'symbol' in locals() else None,
            sources    = [],
            answer     = "",
            latency_ms = latency_ms,
            error      = str(e),
        )
        logger.error(f"intelligence query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))