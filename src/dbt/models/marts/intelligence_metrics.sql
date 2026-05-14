-- GOLD layer — business metrics for monitoring dashboard
-- Aggregates SILVER data into daily metrics per symbol

WITH silver AS (
    SELECT * FROM {{ ref('stg_intelligence_logs') }}
),

daily_metrics AS (
    SELECT
        LOG_DATE,
        SYMBOL,

        -- Volume metrics
        COUNT(*)                                AS TOTAL_QUERIES,

        -- Latency metrics
        ROUND(AVG(LATENCY_MS), 0)               AS AVG_LATENCY_MS,
        MIN(LATENCY_MS)                         AS MIN_LATENCY_MS,
        MAX(LATENCY_MS)                         AS MAX_LATENCY_MS,
        SUM(CASE WHEN SPEED_CATEGORY = 'SLOW'
            THEN 1 ELSE 0 END)                  AS SLOW_QUERIES,

        -- Semantic similarity metrics (THE KEY METRIC)
        ROUND(AVG(AVG_SIMILARITY_SCORE), 4)     AS AVG_SIMILARITY_SCORE,
        ROUND(MIN(TOP_SIMILARITY_SCORE), 4)     AS MIN_SIMILARITY_SCORE,
        ROUND(MAX(TOP_SIMILARITY_SCORE), 4)     AS MAX_SIMILARITY_SCORE,

        -- Sentiment distribution
        SUM(CASE WHEN TOP_ARTICLE_SENTIMENT = 'Positive'
            THEN 1 ELSE 0 END)                  AS POSITIVE_RESULTS,
        SUM(CASE WHEN TOP_ARTICLE_SENTIMENT = 'Negative'
            THEN 1 ELSE 0 END)                  AS NEGATIVE_RESULTS,
        SUM(CASE WHEN TOP_ARTICLE_SENTIMENT = 'Neutral'
            THEN 1 ELSE 0 END)                  AS NEUTRAL_RESULTS,

        -- Response quality
        ROUND(AVG(RESPONSE_LENGTH), 0)          AS AVG_RESPONSE_LENGTH,

        -- Model used
        MODEL

    FROM silver
    GROUP BY LOG_DATE, SYMBOL, MODEL
)

SELECT * FROM daily_metrics
ORDER BY LOG_DATE DESC, TOTAL_QUERIES DESC