"""
Pydantic request models — validates and documents every API input.
FastAPI automatically rejects requests that don't match these shapes.
"""

from typing import Optional
from pydantic import BaseModel, Field


class IntelligenceRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=3,
        max_length=500,
        description="Question to ask about the stock market"
    )
    symbol: Optional[str] = Field(
        None,
        description="Optional stock symbol filter e.g. AAPL"
    )
    top_k: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Number of Pinecone results to retrieve"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "question": "What is the outlook for AAPL?",
                "symbol": "AAPL",
                "top_k": 5
            }
        }


class SQLRequest(BaseModel):
    sql:      str = Field(..., min_length=5, description="SQL query to run")
    database: str = Field(
        default="STOCKMARKETBATCH",
        description="Snowflake database to query"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "sql": "SELECT * FROM HISTORICAL_STOCK LIMIT 10",
                "database": "STOCKMARKETBATCH"
            }
        }


class NLSQLRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=500,
        description="Natural language question about the data"
    )
    database: str = Field(
        default="STOCKMARKETBATCH",
        description="Snowflake database to query"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "question": "Show me AAPL close price for last 30 days",
                "database": "STOCKMARKETBATCH"
            }
        }