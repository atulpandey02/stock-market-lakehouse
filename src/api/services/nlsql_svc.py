"""
NL2SQL service — converts natural language to SQL using Groq.

Flow:
1. Build schema context (table names, columns)
2. Send to Groq with few-shot examples
3. Extract clean SQL from response
4. Validate (SELECT only, no destructive statements)
5. Return generated SQL for execution

Why schema context matters:
  Without it, Groq hallucinates column names.
  With it, Groq generates accurate SQL against your actual tables.
"""

import re
import logging
import requests as http_requests

from api.config import GROQ_API_KEY, GROQ_BASE_URL

logger = logging.getLogger(__name__)

# ── Schema context — tells Groq exactly what tables and columns exist ─────────
# This is the key to accurate NL2SQL — without this, Groq guesses column names

SCHEMA_CONTEXT = """
You are a SQL expert. Generate Snowflake SQL queries for a stock market database.

DATABASE: STOCKMARKETBATCH
TABLES:

1. HISTORICAL_STOCK
   Columns: SYMBOL (VARCHAR), DATE (DATE), OPEN_PRICE (FLOAT), HIGH_PRICE (FLOAT),
            LOW_PRICE (FLOAT), CLOSE_PRICE (FLOAT), VOLUME (INT),
            DAILY_RETURN_PCT (FLOAT), IS_POSITIVE_DAY (BOOLEAN),
            SMA_5 (FLOAT), SMA_20 (FLOAT), BATCH_DATE (DATE)
   Example rows: AAPL, 2026-05-08, 290.01, 294.76, 290.0, 293.32, 52692761, 1.14, TRUE, 285.86, 273.21

2. STOCK_PERFORMANCE (dbt mart — BUY/SELL signals)
   Columns: SYMBOL (VARCHAR), TRADE_DATE (DATE), CLOSE_PRICE (FLOAT),
            DAILY_RETURN_PCT (FLOAT), SMA_5 (FLOAT), SMA_20 (FLOAT),
            SMA_SIGNAL (VARCHAR), OVERALL_SIGNAL (VARCHAR)
   Values for SMA_SIGNAL: 'BULLISH', 'BEARISH'
   Values for OVERALL_SIGNAL: 'BUY', 'SELL', 'HOLD'

3. STOCK_DAILY_METRICS
   Columns: SYMBOL (VARCHAR), DATE (DATE), CLOSE_PRICE (FLOAT),
            DAILY_RETURN_PCT (FLOAT), SMA_5 (FLOAT), SMA_20 (FLOAT)

DATABASE: STOCKMARKETSTREAM
TABLES:

4. REALTIME_STOCK
   Columns: SYMBOL (VARCHAR), WINDOW_START (TIMESTAMP), MA_15M (FLOAT),
            MA_1H (FLOAT), VOLATILITY_15M (FLOAT), VOLUME_SUM_1H (INT)

RULES:
- Always use uppercase for SYMBOL values: 'AAPL' not 'aapl'
- Always add LIMIT 100 unless user asks for specific count
- Use DATE column for HISTORICAL_STOCK date filters
- Use TRADE_DATE column for STOCK_PERFORMANCE date filters
- For "last N days" use: DATE >= DATEADD(day, -N, CURRENT_DATE())
- For "latest" or "most recent" use ORDER BY DATE DESC LIMIT 1
- Only generate SELECT statements — never INSERT, UPDATE, DELETE, DROP

EXAMPLES:

Q: Show me AAPL close price for the last 30 days
A: SELECT DATE, SYMBOL, CLOSE_PRICE, DAILY_RETURN_PCT FROM HISTORICAL_STOCK WHERE SYMBOL = 'AAPL' AND DATE >= DATEADD(day, -30, CURRENT_DATE()) ORDER BY DATE DESC LIMIT 100;

Q: Which stocks have a BUY signal?
A: SELECT SYMBOL, TRADE_DATE, CLOSE_PRICE, SMA_5, SMA_20, OVERALL_SIGNAL FROM STOCK_PERFORMANCE WHERE OVERALL_SIGNAL = 'BUY' ORDER BY SYMBOL;

Q: Show me the top 5 stocks by average daily return
A: SELECT SYMBOL, ROUND(AVG(DAILY_RETURN_PCT), 4) AS AVG_RETURN, COUNT(*) AS TRADING_DAYS FROM HISTORICAL_STOCK GROUP BY SYMBOL ORDER BY AVG_RETURN DESC LIMIT 5;

Q: What is TSLA's highest price ever?
A: SELECT SYMBOL, MAX(HIGH_PRICE) AS ALL_TIME_HIGH, MIN(DATE) AS FIRST_DATE, MAX(DATE) AS LAST_DATE FROM HISTORICAL_STOCK WHERE SYMBOL = 'TSLA' GROUP BY SYMBOL;

Q: Show me latest realtime data for all stocks
A: SELECT SYMBOL, WINDOW_START, MA_15M, MA_1H, VOLATILITY_15M FROM REALTIME_STOCK QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY WINDOW_START DESC) = 1 ORDER BY SYMBOL;
"""


def generate_sql(question: str, database: str = "STOCKMARKETBATCH") -> str:
    """
    Convert natural language question to SQL using Groq.

    Returns clean SQL string ready for execution.
    Raises ValueError if generated SQL is unsafe.
    """
    if not GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY not configured")

    prompt = f"""{SCHEMA_CONTEXT}

Now generate SQL for this question:
Q: {question}
A:"""

    try:
        response = http_requests.post(
            GROQ_BASE_URL,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       "llama-3.3-70b-versatile",
                "messages": [
                    {
                        "role":    "system",
                        "content": "You are a SQL expert. Return ONLY the SQL query, nothing else. No explanation, no markdown, no backticks. Just the raw SQL statement ending with a semicolon."
                    },
                    {
                        "role":    "user",
                        "content": prompt,
                    }
                ],
                "temperature": 0.0,  # Zero temperature — deterministic SQL generation
                "max_tokens":  300,
            },
            timeout=30,
        )

        if response.status_code != 200:
            raise ValueError(f"Groq API error: {response.status_code}")

        raw = response.json()["choices"][0]["message"]["content"].strip()

        # Clean up any markdown code blocks Groq might add
        sql = _clean_sql(raw)

        # Validate before returning
        _validate_sql(sql)

        return sql

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"NL2SQL generation error: {e}")
        raise ValueError(f"Failed to generate SQL: {str(e)}")


def _clean_sql(raw: str) -> str:
    """
    Remove markdown formatting Groq sometimes adds.
    Strip ```sql ... ``` blocks, extra whitespace, etc.
    """
    # Remove markdown code blocks
    raw = re.sub(r"```sql\s*", "", raw)
    raw = re.sub(r"```\s*", "", raw)

    # Remove leading/trailing whitespace
    raw = raw.strip()

    # Take only the first statement if multiple returned
    if ";" in raw:
        raw = raw.split(";")[0] + ";"

    return raw


def _validate_sql(sql: str) -> None:
    """
    Safety validation — ensure only SELECT statements.
    Raises ValueError if dangerous SQL detected.
    """
    sql_upper = sql.upper().strip()

    # Must start with SELECT
    if not sql_upper.startswith("SELECT"):
        raise ValueError(
            f"Generated SQL must start with SELECT. Got: {sql[:50]}"
        )

    # Block dangerous keywords
    dangerous = ["DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "ALTER",
                 "CREATE", "GRANT", "REVOKE", "EXECUTE", "EXEC"]
    for keyword in dangerous:
        if keyword in sql_upper:
            raise ValueError(
                f"Generated SQL contains blocked keyword: {keyword}"
            )