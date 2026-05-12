"""
Pipeline router — aggregate KPIs across both databases.

Endpoints:
  GET /api/v1/pipeline/kpis
"""

import logging

from fastapi import APIRouter, HTTPException

from api.models.responses import PipelineKPIs
import api.services.snowflake as sf_svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get(
    "/kpis",
    response_model=PipelineKPIs,
    summary="Get pipeline KPIs",
    description="Aggregate metrics: row counts, symbol counts, latest batch date, realtime windows"
)
async def get_kpis():
    try:
        return sf_svc.get_pipeline_kpis()
    except Exception as e:
        logger.error(f"kpis endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))