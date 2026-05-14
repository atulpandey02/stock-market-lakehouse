-- SILVER layer — clean and extract from BRONZE RAW_INTELLIGENCE_LOGS
-- Extracts nested JSON fields, validates data, calculates derived fields

WITH raw AS (
    SELECT * FROM {{ source('batch', 'RAW_INTELLIGENCE_LOGS') }}
    WHERE ERROR_MESSAGE IS NULL  -- exclude failed requests
),

extracted AS (
    SELECT
        LOG_ID,
        TIMESTAMP,
        QUESTION,
        SYMBOL,
        TOP_K,
        LATENCY_MS,
        MODEL,
        API_VERSION,
        GROQ_RESPONSE,
        LENGTH(GROQ_RESPONSE)                        AS RESPONSE_LENGTH,

        -- Extract top Pinecone result
        PINECONE_RESULTS[0]:title::STRING            AS TOP_ARTICLE_TITLE,
        PINECONE_RESULTS[0]:source::STRING           AS TOP_ARTICLE_SOURCE,
        PINECONE_RESULTS[0]:sentiment::STRING        AS TOP_ARTICLE_SENTIMENT,
        PINECONE_RESULTS[0]:score::FLOAT             AS TOP_SIMILARITY_SCORE,
        PINECONE_RESULTS[0]:published_at::STRING     AS TOP_ARTICLE_DATE,

        -- Average similarity score across all results
        (
            COALESCE(PINECONE_RESULTS[0]:score::FLOAT, 0) +
            COALESCE(PINECONE_RESULTS[1]:score::FLOAT, 0) +
            COALESCE(PINECONE_RESULTS[2]:score::FLOAT, 0) +
            COALESCE(PINECONE_RESULTS[3]:score::FLOAT, 0) +
            COALESCE(PINECONE_RESULTS[4]:score::FLOAT, 0)
        ) / NULLIF(TOP_K, 0)                         AS AVG_SIMILARITY_SCORE,

        -- Speed category
        CASE
            WHEN LATENCY_MS < 3000  THEN 'FAST'
            WHEN LATENCY_MS < 8000  THEN 'MEDIUM'
            ELSE 'SLOW'
        END                                          AS SPEED_CATEGORY,

        -- Date parts for easy grouping
        DATE(TIMESTAMP)                              AS LOG_DATE,
        HOUR(TIMESTAMP)                              AS LOG_HOUR

    FROM raw
)

SELECT * FROM extracted