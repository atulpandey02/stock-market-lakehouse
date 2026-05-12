#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark Streaming Processor for Real-Time Stock Data
Reads from MinIO raw/realtime/year=/month=/day/, computes windowed metrics,
writes to Iceberg table: iceberg.stock_market.realtime_stocks
"""

import logging
import sys
import traceback
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                StructField, StructType)

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET     = "stock-market-data"
MINIO_ENDPOINT   = "http://minio:9000"

# Iceberg table name — separate from historical_stocks
ICEBERG_TABLE = "iceberg.stock_market.realtime_stocks"

SEPARATOR = "=" * 65
MINI_SEP  = "-" * 65


def get_today():
    """Get today's date in EST without pytz dependency."""
    est = timezone(timedelta(hours=-4))
    return datetime.now(est)


def log_section(title: str):
    """Print a clean section header to logger."""
    logger.info(SEPARATOR)
    logger.info(f"  {title}")
    logger.info(SEPARATOR)


def create_spark_session():
    log_section("INITIALIZING SPARK SESSION")

    spark = (SparkSession.builder
        .appName("StockMarketStreamingProcessor")
        # ── JAR: Iceberg only — hadoop-aws is baked into the Docker image ──
        .config("spark.jars",
                "/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar")
        # ── Iceberg extensions ──────────────────────────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # ── Iceberg REST catalog ────────────────────────────────────────────
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.warehouse",
                "s3://stock-market-data/iceberg")
        # ── Streaming config ────────────────────────────────────────────────
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())

    spark.conf.set("spark.sql.shuffle.partitions", 2)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",        MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key",        MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint",          MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("  Spark Session     : OK")
    logger.info("  MinIO Endpoint    : http://minio:9000")
    logger.info("  Iceberg Catalog   : http://iceberg-rest:8181")
    logger.info("  Iceberg Table     : iceberg.stock_market.realtime_stocks")
    logger.info("  Executor Memory   : 1g")
    logger.info("  Executor Cores    : 2")
    logger.info("  Shuffle Partitions: 2")
    logger.info(SEPARATOR)
    return spark


def define_schema():
    return StructType([
        StructField("symbol",         StringType(), False),
        StructField("price",          DoubleType(), True),
        StructField("change",         DoubleType(), True),
        StructField("percent_change", DoubleType(), True),
        StructField("volume",         IntegerType(), True),
        StructField("timestamp",      StringType(), True),
    ])


def read_stream_from_s3(spark):
    log_section("SETTING UP STREAMING READ")

    schema = define_schema()
    today  = get_today()

    s3_path = (
        f"s3a://{MINIO_BUCKET}/raw/realtime/"
        f"year={today.year}/month={today.month:02d}/day={today.day:02d}/"
    )

    logger.info(f"  Source Path : {s3_path}")
    logger.info(f"  Format      : CSV with header")
    logger.info(f"  Watermark   : 5 minutes")
    logger.info(SEPARATOR)

    try:
        streaming_df = (spark.readStream
            .schema(schema)
            .option("header", "true")
            .csv(s3_path))

        streaming_df = (streaming_df
            .withColumn("timestamp",      F.to_timestamp("timestamp"))
            .withColumn("price",          F.col("price").cast(DoubleType()))
            .withColumn("change",         F.col("change").cast(DoubleType()))
            .withColumn("percent_change", F.col("percent_change").cast(DoubleType()))
            .withColumn("volume",         F.col("volume").cast(IntegerType())))

        return streaming_df

    except Exception as e:
        logger.error(f"Error setting up streaming read: {e}")
        logger.error(traceback.format_exc())
        return None


def process_streaming_data(streaming_df):
    if streaming_df is None:
        return None
    try:
        return streaming_df.withWatermark("timestamp", "5 minutes")
    except Exception as e:
        logger.error(f"Error setting up watermark: {e}")
        return None


def ensure_iceberg_table(spark):
    """
    Create the Iceberg namespace and table if they don't exist yet.
    Called once before the streaming query starts.
    Using append() means this table accumulates all micro-batches over time.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.stock_market")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            symbol          STRING,
            window_start    TIMESTAMP,
            window_15m_end  TIMESTAMP,
            window_1h_end   TIMESTAMP,
            ma_15m          DOUBLE,
            ma_1h           DOUBLE,
            volatility_15m  DOUBLE,
            volatility_1h   DOUBLE,
            volume_sum_15m  LONG,
            volume_sum_1h   LONG
        )
        USING iceberg
        PARTITIONED BY (symbol)
    """)
    logger.info(f"  Iceberg table ready: {ICEBERG_TABLE}")


def process_and_write_batch(df, batch_id):
    """
    foreachBatch handler — runs every 60 seconds.
    Uses .append() NOT .createOrReplace() — streaming data must accumulate,
    not be replaced on every micro-batch.
    """
    count = df.count()

    logger.info(SEPARATOR)
    logger.info(f"  MICRO-BATCH #{batch_id}")
    logger.info(MINI_SEP)
    logger.info(f"  Trigger Time    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Incoming Rows   : {count}")

    if count == 0:
        logger.info("  Status          : No new data — skipping")
        logger.info(SEPARATOR)
        return

    try:
        # ── 15 minute window ──────────────────────────────────────────────
        df_15min = (df
            .groupBy(
                F.col("symbol"),
                F.window("timestamp", "15 minutes", "5 minutes").alias("window")
            )
            .agg(
                F.avg("price").alias("ma_15m"),
                F.stddev("price").alias("volatility_15m"),
                F.sum("volume").alias("volume_sum_15m"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end",   F.col("window.end"))
            .drop("window"))

        # ── 1 hour window ─────────────────────────────────────────────────
        df_1h = (df
            .groupBy(
                F.col("symbol"),
                F.window("timestamp", "1 hour", "10 minutes").alias("window")
            )
            .agg(
                F.avg("price").alias("ma_1h"),
                F.stddev("price").alias("volatility_1h"),
                F.sum("volume").alias("volume_sum_1h"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end",   F.col("window.end"))
            .drop("window"))

        # ── Join both windows ─────────────────────────────────────────────
        result = (df_15min
            .join(df_1h, ["symbol", "window_start"], "inner")
            .select(
                df_15min["symbol"],
                df_15min["window_start"],
                df_15min["window_end"].alias("window_15m_end"),
                df_1h["window_end"].alias("window_1h_end"),
                df_15min["ma_15m"],
                df_1h["ma_1h"],
                df_15min["volatility_15m"],
                df_1h["volatility_1h"],
                df_15min["volume_sum_15m"],
                df_1h["volume_sum_1h"],
            ))

        result_count = result.count()

        if result_count == 0:
            logger.info("  Windowed Rows   : 0 — windows not yet complete")
            logger.info("  Status          : Waiting for more data")
            logger.info(SEPARATOR)
            return

        # ── Log sample results ────────────────────────────────────────────
        logger.info(f"  Windowed Rows   : {result_count}")
        logger.info(MINI_SEP)
        logger.info("  SAMPLE PROCESSED DATA (top 5 rows):")
        logger.info(MINI_SEP)

        rows = result.orderBy("symbol", "window_start").limit(5).collect()
        for row in rows:
            logger.info(
                f"  Symbol: {row['symbol']:<6} | "
                f"Window: {str(row['window_start'])[11:16]} → {str(row['window_15m_end'])[11:16]} | "
                f"MA_15m: ${row['ma_15m']:.2f} | "
                f"MA_1h: ${row['ma_1h']:.2f} | "
                f"Vol_15m: {int(row['volume_sum_15m']):,}"
            )

        # ── Symbol summary ────────────────────────────────────────────────
        logger.info(MINI_SEP)
        logger.info("  SYMBOL SUMMARY:")
        logger.info(MINI_SEP)
        symbols = result.select("symbol").distinct().collect()
        for s in symbols:
            sym_rows = result.filter(F.col("symbol") == s["symbol"])
            latest   = sym_rows.orderBy(F.col("window_start").desc()).first()
            logger.info(
                f"  {s['symbol']:<6} → "
                f"Latest MA_15m: ${latest['ma_15m']:.2f} | "
                f"Latest MA_1h: ${latest['ma_1h']:.2f} | "
                f"Volatility: {latest['volatility_15m']:.4f}"
            )

        # ── Write to Iceberg (append — accumulates all micro-batches) ─────
        logger.info(MINI_SEP)
        logger.info(f"  Writing to Iceberg: {ICEBERG_TABLE}")

        (result
            .writeTo(ICEBERG_TABLE)
            .using("iceberg")
            .append())

        logger.info(f"  Status          : ✓ Written successfully to Iceberg")
        logger.info(SEPARATOR)

    except Exception as e:
        logger.error(f"  ERROR in batch {batch_id}: {e}")
        logger.error(traceback.format_exc())
        logger.info(SEPARATOR)


def write_stream_to_s3(streaming_df):
    log_section("STARTING STREAMING QUERY")

    if streaming_df is None:
        logger.error("No streaming DataFrame to write")
        return None

    checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/streaming_processor"
    logger.info(f"  Trigger         : Every 1 minute")
    logger.info(f"  Output Mode     : append")
    logger.info(f"  Checkpoint      : {checkpoint_path}")
    logger.info(SEPARATOR)

    try:
        query = (streaming_df.writeStream
                 .foreachBatch(process_and_write_batch)
                 .trigger(processingTime="1 minute")
                 .option("checkpointLocation", checkpoint_path)
                 .outputMode("append")
                 .start())

        logger.info("  Streaming query started successfully ✓")
        logger.info(SEPARATOR)
        return query

    except Exception as e:
        logger.error(f"Error starting streaming write: {e}")
        logger.error(traceback.format_exc())
        return None


def main():
    logger.info(SEPARATOR)
    logger.info("  STOCK MARKET REAL-TIME STREAMING PROCESSOR")
    logger.info(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Pipeline  : Kafka → MinIO → Spark → Iceberg")
    logger.info(SEPARATOR)

    spark = create_spark_session()

    try:
        # Create Iceberg table once before streaming starts
        ensure_iceberg_table(spark)

        streaming_df = read_stream_from_s3(spark)
        if streaming_df is None:
            logger.error("Failed to set up streaming read — exiting")
            return

        streaming_df = process_streaming_data(streaming_df)
        if streaming_df is None:
            logger.error("Failed to set up watermark — exiting")
            return

        query = write_stream_to_s3(streaming_df)

        if query:
            log_section("STREAMING PROCESSOR RUNNING")
            logger.info("  Waiting for new files every 60 seconds...")
            logger.info("  Press Ctrl+C to stop gracefully")
            logger.info(SEPARATOR)
            query.awaitTermination()
        else:
            logger.error("Failed to start streaming query")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
    finally:
        log_section("SHUTTING DOWN")
        logger.info("  Stopping Spark session...")
        spark.stop()
        logger.info("  Spark session stopped ✓")
        logger.info(SEPARATOR)


if __name__ == "__main__":
    main()