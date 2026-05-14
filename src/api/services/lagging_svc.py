"""
Logging service — writes GenAI events to Snowflake BRONZE layer.

Every intelligence query gets logged here:
  question, symbol, pinecone results, groq response, latency

This is the foundation of the Medallion architecture:
  BRONZE (this file) → SILVER (dbt clean) → GOLD (dbt aggregates)
"""

import json
import logging
import time
import traceback
from typing import Optional

from api.config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DB_BATCH,
)

logger = logging.getLogger(__name__)


def log_intelligence_event(
    question:     str,
    symbol:       Optional[str],
    sources:      list,
    answer:       str,
    latency_ms:   int,
    top_k:        int   = 5,
    model:        str   = "llama-3.3-70b-versatile",
    api_version:  str   = "v1",
    error:        str | None  = None,
) -> None:
    """
    Write a GenAI intelligence event to RAW_INTELLIGENCE_LOGS (BRONZE layer).

    Called from intelligence router after every query.
    Fails silently — logging should never break the main response.

    Args:
        question:    The user's question
        symbol:      Stock symbol filter if provided
        sources:     Pinecone results (list of dicts)
        answer:      Groq generated response
        latency_ms:  Total time taken in milliseconds
        top_k:       Number of Pinecone results requested
        model:       Groq model used
        api_version: API version
        error:       Error message if something failed
    """
    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account   = SNOWFLAKE_ACCOUNT,
            user      = SNOWFLAKE_USER,
            password  = SNOWFLAKE_PASSWORD,
            role      = SNOWFLAKE_ROLE,
            warehouse = SNOWFLAKE_WAREHOUSE,
            database  = SNOWFLAKE_DB_BATCH,
            schema    = "PUBLIC",
        )
        cur = conn.cursor()

        # Serialize Pinecone results to JSON string for VARIANT column
        pinecone_json = json.dumps([
            {
                "title":        s.get("title", ""),
                "symbol":       s.get("symbol", ""),
                "source":       s.get("source", ""),
                "sentiment":    s.get("sentiment", ""),
                "score":        s.get("score", 0.0),
                "published_at": s.get("published_at", ""),
            }
            for s in sources
        ])

        cur.execute("""
                    INSERT INTO RAW_INTELLIGENCE_LOGS (
                        QUESTION, SYMBOL, TOP_K, PINECONE_RESULTS,
                        GROQ_RESPONSE, LATENCY_MS, MODEL, API_VERSION, ERROR_MESSAGE
                    ) SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s
                """, (question, symbol, top_k, pinecone_json, answer, latency_ms, model, api_version, error))

        conn.close()
        logger.info(f"Logged intelligence event | symbol={symbol} | latency={latency_ms}ms")

    except Exception as e:
        # Silent failure — logging must never break the main API response
        logger.warning(f"Failed to log intelligence event: {e}")
        logger.warning(traceback.format_exc())