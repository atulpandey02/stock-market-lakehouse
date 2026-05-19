"""
RAG Market Intelligence Pipeline — Finnhub + Groq Edition
Fetches financial news → chunks → embeds (HuggingFace sentence-transformers)
→ stores in Pinecone → semantic search → Groq LLM generates answer

Full RAG Pipeline:
  R = Retrieval  → Pinecone similarity search
  A = Augmented  → retrieved chunks injected into prompt
  G = Generation → Groq LLM generates grounded answer

Why Groq:
  ✅ Free tier (30 requests/min)
  ✅ 500 tokens/sec — extremely fast
  ✅ Llama3-8b — capable open source model
  ✅ OpenAI-compatible API format
  ✅ Get key free at console.groq.com
"""

import hashlib
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
FINNHUB_API_KEY     = os.getenv('FINNHUB_API_KEY')
FINNHUB_BASE_URL    = "https://finnhub.io/api/v1"
PINECONE_API_KEY    = os.getenv('PINECONE_API_KEY')
PINECONE_INDEX_NAME = os.getenv('PINECONE_INDEX_NAME', 'stock-market-rag')

# ✅ Groq — free LLM API (get key at console.groq.com)
GROQ_API_KEY        = os.getenv('GROQ_API_KEY')
GROQ_MODEL = "llama-3.1-8b-instant"  # free, fast, capable
GROQ_BASE_URL       = "https://api.groq.com/openai/v1/chat/completions"

# ✅ Local embedding model — no API needed
EMBEDDING_MODEL     = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM       = 384

STOCKS    = ["AAPL","MSFT","GOOGL","AMZN","META","TSLA","NVDA","INTC","JPM","V"]
SEPARATOR = "=" * 65
MINI_SEP  = "-" * 65
CHUNK_SIZE    = 500
CHUNK_OVERLAP = 50


def log_section(title: str):
    logger.info(SEPARATOR)
    logger.info(f"  {title}")
    logger.info(SEPARATOR)


# ── Step 1: Fetch News from Finnhub ──────────────────────────────────────────

def fetch_news_for_symbol(symbol: str, days_back: int = 7) -> List[Dict]:
    """Fetch company news from Finnhub. Free: 60 calls/min."""
    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY not set in .env")

    now       = datetime.now(timezone.utc)
    date_to   = now.strftime("%Y-%m-%d")
    date_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")

    try:
        response = requests.get(
            f"{FINNHUB_BASE_URL}/company-news",
            params  = {"symbol": symbol, "from": date_from, "to": date_to},
            headers = {"X-Finnhub-Token": FINNHUB_API_KEY},
            timeout = 10
        )
        response.raise_for_status()
        articles = response.json()

        if not articles:
            logger.warning(f"  No news for {symbol} ({date_from} → {date_to})")
            return []

        results = []
        for article in articles[:15]:
            headline = article.get("headline", "")
            summary  = article.get("summary",  "")
            if not headline and not summary:
                continue
            results.append({
                "symbol":       symbol,
                "title":        headline,
                "summary":      summary or headline,
                "url":          article.get("url",    ""),
                "source":       article.get("source", ""),
                "published_at": datetime.fromtimestamp(
                    article.get("datetime", 0), tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "sentiment":    _derive_sentiment(headline + " " + summary),
                "relevance":    1.0,
            })

        logger.info(f"  Fetched {len(results)} articles for {symbol}")
        return results

    except Exception as e:
        logger.error(f"  Error fetching {symbol}: {e}")
        return []


def _derive_sentiment(text: str) -> str:
    text_lower = text.lower()
    positive   = ["beat","surge","rise","gain","growth","profit","strong",
                  "record","outperform","upgrade","bullish","rally","boost"]
    negative   = ["miss","fall","drop","loss","decline","weak","downgrade",
                  "bearish","cut","warn","risk","concern","slump","crash"]
    pos = sum(1 for w in positive if w in text_lower)
    neg = sum(1 for w in negative if w in text_lower)
    if pos > neg: return "Positive"
    if neg > pos: return "Negative"
    return "Neutral"


def fetch_all_news(days_back: int = 7) -> List[Dict]:
    log_section("FETCHING FINANCIAL NEWS (Finnhub)")
    all_articles = []
    for i, symbol in enumerate(STOCKS):
        articles = fetch_news_for_symbol(symbol, days_back=days_back)
        all_articles.extend(articles)
        if i < len(STOCKS) - 1:
            time.sleep(0.5)
    logger.info(f"  Total articles fetched: {len(all_articles)}")
    return all_articles


# ── Step 2: Chunk Text ────────────────────────────────────────────────────────

def chunk_text(text: str,
               chunk_size: int = CHUNK_SIZE,
               overlap: int = CHUNK_OVERLAP) -> List[str]:
    if not text or len(text) < chunk_size:
        return [text] if text else []
    chunks, start = [], 0
    while start < len(text):
        chunks.append(text[start:start + chunk_size])
        start += chunk_size - overlap
    return chunks


def prepare_documents(articles: List[Dict]) -> List[Dict]:
    log_section("PREPARING DOCUMENTS FOR EMBEDDING")
    documents = []
    for article in articles:
        full_text = f"Title: {article['title']}\n\nSummary: {article['summary']}"
        chunks    = chunk_text(full_text)
        for i, chunk in enumerate(chunks):
            doc_id = hashlib.md5(f"{article['url']}-{i}".encode()).hexdigest()
            documents.append({
                "id":   doc_id,
                "text": chunk,
                "metadata": {
                    "symbol":       article["symbol"],
                    "title":        article["title"][:200],
                    "summary":      article["summary"][:300],
                    "source":       article["source"],
                    "url":          article["url"],
                    "published_at": article["published_at"],
                    "sentiment":    article["sentiment"],
                    "chunk_index":  i,
                    "total_chunks": len(chunks),
                }
            })
    logger.info(f"  Prepared {len(documents)} chunks from {len(articles)} articles")
    return documents


# ── Step 3: Embed Locally with HuggingFace ───────────────────────────────────

def get_embeddings(texts: List[str]) -> List[List[float]]:
    """
    Embed texts using sentence-transformers locally.
    all-MiniLM-L6-v2 = embedding model (NOT LLM)
    Converts text → 384 numbers capturing semantic meaning.
    Runs on CPU — free, no API key needed.
    """
    import numpy as np
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBEDDING_MODEL)
    vectors = model.encode(texts, show_progress_bar=True)
    return np.asarray(vectors).tolist()


# ── Step 4: Store in Pinecone ─────────────────────────────────────────────────

def get_pinecone_index():
    from pinecone import Pinecone, ServerlessSpec
    pc    = Pinecone(api_key=PINECONE_API_KEY)
    names = [idx.name for idx in pc.list_indexes()]
    if PINECONE_INDEX_NAME not in names:
        logger.info(f"  Creating Pinecone index: {PINECONE_INDEX_NAME}")
        pc.create_index(
            name=PINECONE_INDEX_NAME, dimension=EMBEDDING_DIM,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
        time.sleep(30)
    return pc.Index(PINECONE_INDEX_NAME)


def store_in_pinecone(documents: List[Dict], batch_size: int = 100):
    log_section("EMBEDDING (HuggingFace local) + STORING IN PINECONE")
    logger.info(f"  Embedding model : {EMBEDDING_MODEL}")
    logger.info(f"  Dimensions      : {EMBEDDING_DIM}")
    logger.info(f"  Documents       : {len(documents)}")

    index = get_pinecone_index()
    for i in range(0, len(documents), batch_size):
        batch      = documents[i:i + batch_size]
        logger.info(f"  Embedding batch {i // batch_size + 1} ({len(batch)} chunks)...")
        embeddings = get_embeddings([doc["text"] for doc in batch])
        vectors    = [
            {"id": doc["id"], "values": emb, "metadata": doc["metadata"]}
            for doc, emb in zip(batch, embeddings)
        ]
        index.upsert(vectors=vectors)
        logger.info(f"  ✓ Upserted {len(vectors)} vectors")

    stats = index.describe_index_stats()
    logger.info(f"  Total vectors in Pinecone: {stats.total_vector_count}")


# ── Step 5: Retrieve from Pinecone ───────────────────────────────────────────

def retrieve_relevant_chunks(
    question: str,
    symbol:   Optional[str] = None,
    top_k:    int = 5
) -> List[Dict]:
    """
    Semantic search over Pinecone.
    Embeds question → finds most similar article chunks.
    Returns ranked list of article metadata.
    """
    from sentence_transformers import SentenceTransformer
    from pinecone import Pinecone

    # Embed question using same model as ingestion
    model     = SentenceTransformer(EMBEDDING_MODEL)
    query_vec = model.encode([question])[0].tolist()

    pc    = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index(PINECONE_INDEX_NAME)

    query_params = {
        "vector":           query_vec,
        "top_k":            top_k,
        "include_metadata": True
    }
    if symbol and symbol != "All Stocks":
        query_params["filter"] = {"symbol": symbol.upper()}

    results = index.query(**query_params)
    matches = results.get("matches", [])

    # Deduplicate by URL
    seen, unique = set(), []
    for m in matches:
        url = m["metadata"].get("url", "")
        if url not in seen:
            seen.add(url)
            unique.append({
                "title":        m["metadata"].get("title",        ""),
                "summary":      m["metadata"].get("summary",      ""),
                "symbol":       m["metadata"].get("symbol",       ""),
                "source":       m["metadata"].get("source",       ""),
                "url":          url,
                "published_at": m["metadata"].get("published_at", ""),
                "sentiment":    m["metadata"].get("sentiment",    "Neutral"),
                "score":        round(m["score"], 3),
            })
    return unique


# ── Step 6: Generate Answer with Groq LLM ────────────────────────────────────

def generate_answer_with_groq(question: str, chunks: List[Dict]) -> Optional[str]:
    """
    Generate a grounded answer using Groq's free LLM API.

    How it works:
      1. Build context from retrieved Pinecone chunks
      2. Send context + question to Groq (Llama3-8b)
      3. LLM synthesizes answer using ONLY the provided context
         → grounded = no hallucination, only uses real articles

    Groq free tier:
      Model: llama3-8b-8192
      Speed: ~500 tokens/second
      Limit: 30 requests/minute (plenty for portfolio demo)
    """
    if not GROQ_API_KEY:
        logger.warning("  GROQ_API_KEY not set — returning articles only")
        return None

    if not chunks:
        return "No relevant news articles found for this query."

    # Build context from top retrieved chunks
    context = "\n\n".join([
        f"[{c['symbol']}] {c['title']}\n"
        f"Published: {c['published_at']} | Source: {c['source']}\n"
        f"Sentiment: {c['sentiment']}\n"
        f"Summary: {c['summary']}"
        for c in chunks
    ])

    system_prompt = """You are a professional financial market analyst assistant.
Your job is to answer questions about stock market news accurately and concisely.
IMPORTANT RULES:
- Use ONLY the provided news articles to form your answer
- Do NOT use any external knowledge or make up information
- Always cite which articles support your points
- Be concise — 3-5 sentences maximum
- If the context doesn't have enough information, say so clearly"""

    user_prompt = f"""Based on the following recent news articles, answer this question:

Question: {question}

News Articles:
{context}

Please provide a concise analysis based only on the articles above."""

    try:
        response = requests.post(
            GROQ_BASE_URL,
            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type":  "application/json"
            },
            json = {
                "model":       GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt}
                ],
                "temperature": 0.1,       # low = factual, consistent
                "max_tokens":  500,
            },
            timeout = 30
        )
        response.raise_for_status()
        data   = response.json()
        answer = data["choices"][0]["message"]["content"].strip()
        logger.info(f"  ✓ Groq generated {len(answer.split())} word answer")
        return answer

    except requests.exceptions.RequestException as e:
        logger.error(f"  Groq API error: {e}")
        return f"Could not generate answer: {e}"
    except Exception as e:
        logger.error(f"  Unexpected error: {e}")
        return None


# ── Step 7: Full RAG Query ───────────────────────────────────────────────────

def query_market_intelligence(
    question: str,
    symbol:   Optional[str] = None,
    top_k:    int = 5
) -> dict:
    """
    Complete RAG pipeline:
      R → Retrieve relevant chunks from Pinecone
      A → Augment Groq prompt with retrieved context
      G → Generate grounded answer with Llama3-8b

    Returns dict for Streamlit UI to render.
    Falls back to articles-only if GROQ_API_KEY not set.
    """
    log_section("QUERYING MARKET INTELLIGENCE")
    logger.info(f"  Question : {question}")
    logger.info(f"  Symbol   : {symbol or 'All stocks'}")
    logger.info(f"  Model    : {GROQ_MODEL} via Groq")

    # R — Retrieve
    chunks = retrieve_relevant_chunks(question, symbol=symbol, top_k=top_k)
    logger.info(f"  Retrieved: {len(chunks)} relevant chunks")

    # A + G — Augment and Generate
    answer = generate_answer_with_groq(question, chunks)

    return {
        "question":      question,
        "symbol_filter": symbol,
        "answer":        answer,
        "sources":       chunks,
        "total_found":   len(chunks),
    }


def format_query_output(result: dict) -> str:
    """Format result as readable string — for terminal testing."""
    output  = f"\n{'=' * 55}\n  MARKET INTELLIGENCE ANSWER\n{'=' * 55}\n"
    output += f"\n  Q: {result['question']}\n"
    if result['symbol_filter']:
        output += f"  Filter: {result['symbol_filter']}\n"

    if result.get("answer"):
        output += f"\n{'─' * 55}\n  AI ANALYSIS (Groq Llama3)\n{'─' * 55}\n"
        output += f"\n  {result['answer']}\n"
    else:
        output += "\n  ⚠️  No AI answer — add GROQ_API_KEY to .env\n"

    output += f"\n{'─' * 55}\n  SOURCES ({len(result['sources'])}):\n{'─' * 55}\n"
    if not result["sources"]:
        output += "\n  No relevant articles found.\n"
    else:
        for i, r in enumerate(result["sources"], 1):
            icon = "📈" if r["sentiment"]=="Positive" else "📉" if r["sentiment"]=="Negative" else "➖"
            output += (
                f"\n  [{i}] {r['title']}\n"
                f"      {icon} {r['sentiment']:<10} "
                f"Score: {r['score']:.3f}   "
                f"Symbol: {r['symbol']}\n"
                f"      Source    : {r['source']}\n"
                f"      Published : {r['published_at']}\n"
                f"      URL       : {r['url']}\n"
            )
    output += f"\n{'=' * 55}\n"
    return output


# ── Airflow Task ──────────────────────────────────────────────────────────────

def run_news_ingestion(**context):
    """Airflow-callable: fetch news → embed → store in Pinecone."""
    log_section("STARTING RAG NEWS INGESTION (Finnhub)")
    articles = fetch_all_news(days_back=7)
    if not articles:
        logger.warning("  No articles fetched — skipping")
        return
    documents = prepare_documents(articles)
    store_in_pinecone(documents)
    log_section("RAG NEWS INGESTION COMPLETE ✓")
    logger.info(f"  Articles ingested : {len(articles)}")
    logger.info(f"  Chunks stored     : {len(documents)}")
    logger.info(f"  Pinecone index    : {PINECONE_INDEX_NAME}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log_section("RAG MARKET INTELLIGENCE — FULL DEMO")

    # Step 1: Ingest
    run_news_ingestion()

    # Step 2: Demo queries
    demo_questions = [
        ("What is the current market sentiment around Apple?",  "AAPL"),
        ("Are there any concerns about NVIDIA's supply chain?", "NVDA"),
        ("What are analysts saying about Tesla recently?",      "TSLA"),
        ("Give me a summary of overall market conditions.",      None),
    ]
    for question, symbol in demo_questions:
        result = query_market_intelligence(question, symbol=symbol)
        print(format_query_output(result))


if __name__ == "__main__":
    main()