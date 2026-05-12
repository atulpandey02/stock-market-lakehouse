"""
Health endpoint — checks all downstream services are reachable.
GET /api/v1/health

Why this matters:
- In production, load balancers ping /health to know if the service is up
- In your portfolio, it's a quick way to verify all connections work
- Returns "healthy" only if ALL services respond
- Returns "degraded" if any service is down (but still running)
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.models.responses import HealthResponse, ServiceStatus
import api.services.snowflake as sf_svc
import api.services.pinecone_svc as pc_svc

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check all downstream services are reachable"
)
async def health_check():
    services = []

    # Check Snowflake batch DB
    try:
        sf_svc._run_query("SELECT 1", "STOCKMARKETBATCH")
        services.append(ServiceStatus(
            name="snowflake_batch", status="ok", message="STOCKMARKETBATCH reachable"
        ))
    except Exception as e:
        services.append(ServiceStatus(
            name="snowflake_batch", status="error", message=str(e)[:100]
        ))

    # Check Snowflake stream DB
    try:
        sf_svc._run_query("SELECT 1", "STOCKMARKETSTREAM")
        services.append(ServiceStatus(
            name="snowflake_stream", status="ok", message="STOCKMARKETSTREAM reachable"
        ))
    except Exception as e:
        services.append(ServiceStatus(
            name="snowflake_stream", status="error", message=str(e)[:100]
        ))

    # Check Pinecone
    try:
        stats = pc_svc.get_index_stats()
        if stats["status"] == "ok":
            services.append(ServiceStatus(
                name="pinecone",
                status="ok",
                message=f"{stats['total_vectors']:,} vectors indexed"
            ))
        else:
            raise Exception(stats["status"])
    except Exception as e:
        services.append(ServiceStatus(
            name="pinecone", status="error", message=str(e)[:100]
        ))

    # Overall status — degraded if any service is down
    all_ok  = all(s.status == "ok" for s in services)
    overall = "healthy" if all_ok else "degraded"

    status_code = 200 if all_ok else 207  # 207 = Multi-Status

    return JSONResponse(
        status_code=status_code,
        content=HealthResponse(
            status    = overall,
            services  = services,
            timestamp = datetime.now(timezone.utc)
        ).model_dump(mode="json")
    )