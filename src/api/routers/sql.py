"""
SQL router — ad-hoc SQL queries for the SQL Explorer page.

POST /api/v1/sql/query

Security: only SELECT statements allowed.
The service layer blocks DROP, DELETE, TRUNCATE, INSERT, UPDATE, ALTER.
"""

import logging

from fastapi import APIRouter, HTTPException
from api.models.requests import SQLRequest, NLSQLRequest
from api.models.responses import SQLResponse, NLSQLResponse
import api.services.nlsql_svc as nlsql_svc
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

@router.post(
    "/ask",
    response_model=NLSQLResponse,
    summary="Natural language to SQL query",
    description="Ask a question in plain English — Groq generates and executes SQL"
)
async def ask_natural_language(request: NLSQLRequest):
    """
    NL2SQL endpoint:
    1. Groq converts question to SQL
    2. Validate SQL (SELECT only)
    3. Execute against Snowflake
    4. Return results + generated SQL for transparency
    """
    if request.database.upper() not in ALLOWED_DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Database must be one of: {ALLOWED_DATABASES}"
        )

    # Step 1: Generate SQL from natural language
    try:
        generated_sql = nlsql_svc.generate_sql(
            question=request.question,
            database=request.database.upper()
        )
        logger.info(f"NL2SQL: '{request.question}' → {generated_sql[:80]}...")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"NL2SQL generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # Step 2: Execute generated SQL
    try:
        result = sf_svc.run_raw_query(generated_sql, request.database.upper())
        return NLSQLResponse(
            question      = request.question,
            generated_sql = generated_sql,
            row_count     = result.row_count,
            column_count  = result.column_count,
            columns       = result.columns,
            rows          = result.rows,
            database      = request.database.upper(),
        )

    except ValueError as e:
        # SQL validation failed
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"NL2SQL execution error: {e}")
        # Return error with generated SQL so user can see what was attempted
        raise HTTPException(
            status_code=500,
            detail=f"SQL generated but execution failed: {str(e)}\nGenerated SQL: {generated_sql}"
        )