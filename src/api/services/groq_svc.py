"""
Groq service — LLM generation with rate limiting.

Why rate limiting matters:
Groq free tier allows 30 requests/minute.
Without rate limiting, if 10 users hit the API simultaneously
all sending RAG queries, you'd exhaust the limit instantly.
We track timestamps of recent calls and reject if over limit.

Rate limit logic:
- Keep a list of timestamps for recent calls
- On each call: remove timestamps older than 60s
- If remaining count >= limit: reject with 429
- Else: add current timestamp and proceed
"""

import time
import logging
import requests as http_requests
from typing import List
from collections import deque

from api.config import GROQ_API_KEY, GROQ_MODEL, GROQ_BASE_URL, GROQ_RATE_LIMIT_PER_MINUTE
from api.models.responses import NewsSource

logger = logging.getLogger(__name__)

# Sliding window rate limiter
_call_timestamps: deque = deque()


def _check_rate_limit():
    """
    Sliding window rate limiter.
    Removes timestamps older than 60s, then checks if we're at the limit.
    """
    now = time.time()

    # Remove timestamps older than 60 seconds
    while _call_timestamps and now - _call_timestamps[0] > 60:
        _call_timestamps.popleft()

    if len(_call_timestamps) >= GROQ_RATE_LIMIT_PER_MINUTE:
        oldest = _call_timestamps[0]
        wait   = 60 - (now - oldest)
        raise Exception(f"Rate limit reached. Try again in {wait:.0f} seconds.")

    _call_timestamps.append(now)


def generate(
    question:         str,
    sources:          List[NewsSource],
    pipeline_metrics: str = ""
) -> str:
    """
    Generate a grounded answer using Groq LLM.

    Two modes:
    1. Full mode: pipeline_metrics + news context → richer answer
    2. Fallback mode: news only → if Snowflake unavailable

    The system prompt changes based on which data sources are available.
    This graceful degradation is important for production systems.
    """
    if not GROQ_API_KEY:
        return "⚠️ GROQ_API_KEY not configured."

    # Rate limit check before making the API call
    _check_rate_limit()

    # Build news context from Pinecone results
    news_context = "\n\n".join([
        f"[{s.symbol}] {s.title}\n"
        f"Published: {s.published_at} | Source: {s.source}\n"
        f"Sentiment: {s.sentiment}\n"
        f"Summary: {s.summary}"
        for s in sources
    ]) if sources else "No news articles retrieved."

    # System prompt changes based on available data
    if pipeline_metrics:
        system_prompt = """You are a professional financial market analyst with access to \
two data sources:
1. Quantitative signals from a real-time data pipeline (Spark + dbt transformations)
2. Recent financial news articles from a vector database

Synthesise BOTH sources into a coherent analysis.
Rules:
- Always reference the pipeline signal (BULLISH/BEARISH/NEUTRAL) explicitly
- If pipeline signal conflicts with news sentiment, flag this clearly
- If SMA-5 > SMA-20 mention bullish crossover; if SMA-5 < SMA-20 mention bearish
- Be concise — 4-6 sentences
- Do not use external knowledge beyond what is provided"""
    else:
        system_prompt = """You are a professional financial market analyst.
Answer questions using ONLY the provided news articles.
Be concise (3-5 sentences). Cite sources where relevant.
Do not use external knowledge."""

    # User message combines both data sources
    user_content = ""
    if pipeline_metrics:
        user_content += f"QUANTITATIVE PIPELINE DATA:\n{pipeline_metrics}\n\n"
    user_content += f"RECENT NEWS ARTICLES:\n{news_context}\n\n"
    user_content += f"Question: {question}\n\nAnalysis:"

    try:
        response = http_requests.post(
            GROQ_BASE_URL,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_content},
                ],
                "temperature": 0.1,
                "max_tokens":  600,
            },
            timeout=30,
        )

        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"].strip()

        logger.error(f"Groq API error {response.status_code}: {response.text[:200]}")
        return f"⚠️ Groq API error {response.status_code}"

    except http_requests.exceptions.Timeout:
        return "⚠️ Groq API timed out — try again."
    except Exception as e:
        logger.error(f"Groq generate error: {e}")
        return f"⚠️ Error: {str(e)}"