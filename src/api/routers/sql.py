"""
SQL router — ad-hoc SQL queries for the SQL Explorer page.

POST /api/v1/sql/query

Security: only SELECT statements allowed.
The service layer blocks DROP, DELETE, TRUNCATE, INSERT, UPDATE, ALTER.
"""

import logging

from fastapi import APIRouter, HTTPException

from api.models.requests import SQLRequest
from api.models.responses import SQLResponse
import api.services.snowflake as sf_svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sql", tags=["sql"])

ALLOWED_DATABASES = {"STOCKMARKETBATCH", "STOCKMARKETSTREAM"}


@router.post(
    "/query",
    response_model=SQLResponse,
    summary="Run SQL query",
    description="Execute a SELECT query against Snowflake — read-only"
)
async def run_query(request: SQLRequest):
    if request.database.upper() not in ALLOWED_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Database must be one of: {ALLOWED_DATABASES}"
        )

    try:
        return sf_svc.run_raw_query(request.sql, request.database.upper())
    except ValueError as e:
        # Blocked destructive statement
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"sql query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))