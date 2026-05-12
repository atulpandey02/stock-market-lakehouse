"""
Pinecone service — semantic search over financial news vectors.

How it works:
1. Load embedding model (once, cached by @lru_cache)
2. Encode the question into a 384-dim vector
3. Query Pinecone for the most similar vectors
4. Return deduplicated article metadata

The embedding model is the SAME one used during ingestion in rag_pipeline.py
— this is critical. If you embed the question with a different model than
the stored articles, the similarity scores are meaningless.
"""

import logging
from typing import List, Optional
from functools import lru_cache

from api.config import (
    PINECONE_API_KEY, PINECONE_INDEX_NAME, EMBEDDING_MODEL
)
from api.models.responses import NewsSource

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _load_embedding_model():
    """
    Load model once and keep in memory.
    lru_cache(maxsize=1) means it loads on first call,
    then returns the cached instance on every subsequent call.
    Loading takes ~3 seconds — we only want to do this once.
    """
    from sentence_transformers import SentenceTransformer
    logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
    return SentenceTransformer(EMBEDDING_MODEL)


def _get_pinecone_index():
    """Get Pinecone index connection."""
    from pinecone import Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    return pc.Index(PINECONE_INDEX_NAME)


def search(
    question: str,
    symbol:   Optional[str] = None,
    top_k:    int = 5
) -> List[NewsSource]:
    """
    Semantic search — finds articles most similar to the question.

    Steps:
    1. Embed question → 384-dim vector
    2. Query Pinecone with that vector
    3. Filter by symbol if provided
    4. Deduplicate by URL (same article can appear as multiple chunks)
    5. Return as NewsSource Pydantic models
    """
    model     = _load_embedding_model()
    query_vec = model.encode([question])[0].tolist()

    index = _get_pinecone_index()

    params = {
        "vector":           query_vec,
        "top_k":            top_k,
        "include_metadata": True
    }
    if symbol and symbol.upper() != "ALL":
        params["filter"] = {"symbol": symbol.upper()}

    results = index.query(**params)

    # Deduplicate by URL — same article can be chunked into multiple vectors
    seen, unique = set(), []
    for match in results.get("matches", []):
        meta = match.get("metadata") or {}
        url  = str(meta.get("url", ""))

        if url and url not in seen:
            seen.add(url)
            unique.append(NewsSource(
                title        = str(meta.get("title",        "N/A")),
                summary      = str(meta.get("summary",      "")),
                symbol       = str(meta.get("symbol",       "")),
                source       = str(meta.get("source",       "")),
                url          = url,
                published_at = str(meta.get("published_at", "")),
                sentiment    = str(meta.get("sentiment",    "Neutral")),
                score        = float(match.get("score",     0.0)),
            ))

    logger.info(f"Pinecone search: '{question[:50]}' → {len(unique)} results")
    return unique


def get_index_stats() -> dict:
    """Return Pinecone index stats for health check."""
    try:
        index = _get_pinecone_index()
        stats = index.describe_index_stats()
        return {
            "total_vectors": stats.total_vector_count or 0,
            "dimension":     stats.dimension or 384,
            "status":        "ok"
        }
    except Exception as e:
        return {"total_vectors": 0, "dimension": 384, "status": str(e)}