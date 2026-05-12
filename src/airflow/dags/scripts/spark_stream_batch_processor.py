#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark Batch Processor for Real-Time Stock Data
Reads today's raw/realtime CSVs from MinIO, computes windowed metrics,
writes results to processed/realtime as parquet.

Usage:
  Manual:  python spark_stream_batch_processor.py
  Airflow: python spark_stream_batch_processor.py 2026-03-29
           (Airflow passes {{ ds }} which is UTC date)
"""

import logging
import sys
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                StructField, StructType)

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

SEPARATOR = "=" * 65
MINI_SEP  = "-" * 65


def get_process_date():
    """
    Get the date to process using UTC — matches consumer file paths.

    - Airflow run: sys.argv[1] = {{ ds }} e.g. "2026-03-29" (UTC)
    - Manual run:  uses datetime.now(UTC) 

    Returns: (year, month, day) as strings e.g. ("2026", "03", "29")
    """
    if len(sys.argv) > 1:
        # ✅ Airflow passed {{ ds }} — always UTC
        date_str = sys.argv[1]
        logger.info(f"  Date Source : Airflow {{ ds }} = {date_str} (UTC)")
        year, month, day = date_str.split("-")
        return year, month, day
    else:
        # ✅ Manual run — use UTC to match consumer
        now = datetime.now(timezone.utc)
        logger.info(f"  Date Source : Manual run = {now.strftime('%Y-%m-%d')} (UTC)")
        return str(now.year), f"{now.month:02d}", f"{now.day:02d}"


def log_section(title: str):
    logger.info(SEPARATOR)
    logger.info(f"  {title}")
    logger.info(SEPARATOR)


def create_spark_session():
    log_section("INITIALIZING SPARK SESSION")

    spark = (SparkSession.builder
        .appName("StockMarketRealtimeBatchProcessor")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.901")
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


def read_batch_from_s3(spark, year, month, day):
    log_section("READING RAW REALTIME DATA")

    schema  = define_schema()
    s3_path = (
        f"s3a://{MINIO_BUCKET}/raw/realtime/"
        f"year={year}/month={month}/day={day}/"
    )

    logger.info(f"  Source Path : {s3_path}")
    logger.info(f"  Process Date: {year}-{month}-{day} (UTC)")

    try:
        df = (spark.read
              .schema(schema)
              .option("header", "true")
              .option("recursiveFileLookup", "true")
              .csv(s3_path))

        count = df.count()
        if count == 0:
            logger.warning("  Status      : No data found for this date")
            logger.info(SEPARATOR)
            return None

        df = (df
            .withColumn("timestamp",      F.to_timestamp("timestamp"))
            .withColumn("price",          F.col("price").cast(DoubleType()))
            .withColumn("change",         F.col("change").cast(DoubleType()))
            .withColumn("percent_change", F.col("percent_change").cast(DoubleType()))
            .withColumn("volume",         F.col("volume").cast(IntegerType())))

        logger.info(f"  Records Read: {count}")
        logger.info(MINI_SEP)
        logger.info("  SAMPLE RAW DATA (top 5 rows):")
        logger.info(MINI_SEP)

        rows = df.orderBy("timestamp").limit(5).collect()
        for row in rows:
            logger.info(
                f"  {row['symbol']:<6} | "
                f"Price: ${row['price']:.2f} | "
                f"Change: {row['change']:+.2f} | "
                f"Volume: {row['volume']:,} | "
                f"Time: {str(row['timestamp'])[11:19]}"
            )
        logger.info(SEPARATOR)
        return df

    except Exception as e:
        logger.error(f"  Error reading from S3: {e}")
        logger.error(traceback.format_exc())
        return None


def process_batch_data(df):
    log_section("PROCESSING WINDOWED METRICS")

    if df is None:
        return None

    try:
        # Portfolio-friendly window sizes — works with 3-5 min of data
        window_3min = F.window("timestamp", "3 minutes", "1 minute")
        window_5min = F.window("timestamp", "5 minutes", "1 minutes")

        logger.info("  Computing 3-minute windows...")
        df_3min = (df
            .groupBy(F.col("symbol"), window_3min.alias("window"))
            .agg(
                F.avg("price").alias("ma_3m"),
                F.stddev("price").alias("volatility_3m"),
                F.sum("volume").alias("volume_sum_3m"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end",   F.col("window.end"))
            .drop("window"))

        logger.info("  Computing 5-minute windows...")
        df_5min = (df
            .groupBy(F.col("symbol"), window_5min.alias("window"))
            .agg(
                F.avg("price").alias("ma_5m"),
                F.stddev("price").alias("volatility_5m"),
                F.sum("volume").alias("volume_sum_5m"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end",   F.col("window.end"))
            .drop("window"))

        logger.info("  Joining windows...")
        processed_df = (df_3min
            .join(df_5min,
                  (df_3min.symbol       == df_5min.symbol) &
                  (df_3min.window_start == df_5min.window_start),
                  "inner")
            .select(
                df_3min.symbol,
                df_3min.window_start.alias("window_start"),
                df_3min.window_end.alias("window_3m_end"),
                df_5min.window_end.alias("window_5m_end"),
                df_3min.ma_3m,
                df_5min.ma_5m,
                df_3min.volatility_3m,
                df_5min.volatility_5m,
                df_3min.volume_sum_3m,
                df_5min.volume_sum_5m,
            ))

        result_count = processed_df.count()
        logger.info(f"  Windowed Rows: {result_count}")
        logger.info(MINI_SEP)
        logger.info("  SAMPLE PROCESSED DATA (top 5 rows):")
        logger.info(MINI_SEP)

        rows = processed_df.orderBy("symbol", "window_start").limit(5).collect()
        for row in rows:
            logger.info(
                f"  {row['symbol']:<6} | "
                f"Window: {str(row['window_start'])[11:16]} → {str(row['window_3m_end'])[11:16]} | "
                f"MA_3m: ${row['ma_3m']:.2f} | "
                f"MA_5m: ${row['ma_5m']:.2f} | "
                f"Vol: {int(row['volume_sum_3m']):,}"
            )

        logger.info(MINI_SEP)
        logger.info("  SYMBOL SUMMARY (latest window per symbol):")
        logger.info(MINI_SEP)

        symbols = processed_df.select("symbol").distinct().collect()
        for s in symbols:
            latest = (processed_df
                .filter(F.col("symbol") == s["symbol"])
                .orderBy(F.col("window_start").desc())
                .first())
            logger.info(
                f"  {s['symbol']:<6} → "
                f"MA_3m: ${latest['ma_3m']:.2f} | "
                f"MA_5m: ${latest['ma_5m']:.2f} | "
                f"Volatility: {latest['volatility_3m']:.4f}"
            )

        logger.info(SEPARATOR)
        return processed_df

    except Exception as e:
        logger.error(f"  Error processing data: {e}")
        logger.error(traceback.format_exc())
        return None


def write_batch_to_s3(processed_df, year, month, day):
    log_section("WRITING PROCESSED DATA TO MINIO")

    if processed_df is None:
        logger.error("  No processed DataFrame to write")
        return False

    output_path = (
        f"s3a://{MINIO_BUCKET}/processed/realtime/"
        f"year={year}/month={month}/day={day}/"
    )

    logger.info(f"  Output Path : {output_path}")
    logger.info(f"  Write Mode  : overwrite")
    logger.info(f"  Partitioned : by symbol")

    try:
        (processed_df.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(output_path))

        logger.info(f"  Status      : ✓ Written successfully")
        logger.info(SEPARATOR)
        return True

    except Exception as e:
        logger.error(f"  Error writing to S3: {e}")
        logger.error(traceback.format_exc())
        return False


def main():
    year, month, day = get_process_date()

    log_section("STOCK MARKET REALTIME BATCH PROCESSOR")
    logger.info(f"  Process Date: {year}-{month}-{day} (UTC)")
    logger.info(f"  Pipeline    : MinIO (raw/realtime) → Spark → MinIO (processed/realtime)")
    logger.info(f"  Trigger     : {'Airflow' if len(sys.argv) > 1 else 'Manual'}")
    logger.info(SEPARATOR)

    spark = create_spark_session()

    try:
        df = read_batch_from_s3(spark, year, month, day)
        if df is None:
            logger.warning("  No data to process — exiting cleanly")
            return

        processed_df = process_batch_data(df)
        if processed_df is None:
            logger.error("  Processing failed — exiting")
            return

        success = write_batch_to_s3(processed_df, year, month, day)

        if success:
            log_section("BATCH PROCESSING COMPLETE ✓")
            logger.info(f"  Output: s3a://{MINIO_BUCKET}/processed/realtime/year={year}/month={month}/day={day}/")
            logger.info(SEPARATOR)
        else:
            logger.error("  Failed to write processed data")
            sys.exit(1)

    except Exception as e:
        logger.error(f"  Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        log_section("SHUTTING DOWN")
        logger.info("  Stopping Spark session...")
        spark.stop()
        logger.info("  Spark session stopped ✓")
        logger.info(SEPARATOR)


if __name__ == "__main__":
    main()