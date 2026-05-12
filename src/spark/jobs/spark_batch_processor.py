import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def get_output_date():
    """
    Get output partition date.
    - Airflow: sys.argv[1] = {{ ds }} e.g. "2026-03-29" (UTC)
    - Manual:  datetime.now(UTC)
    Ensures Spark output path matches Snowflake load path.
    """
    if len(sys.argv) > 1:
        date_str         = sys.argv[1]
        year, month, day = date_str.split("-")
        print(f"Date source: Airflow ds = {date_str}")
    else:
        now  = datetime.now(timezone.utc)
        year  = str(now.year)
        month = f"{now.month:02d}"
        day   = f"{now.day:02d}"
        print(f"Date source: UTC now = {year}-{month}-{day}")
    return year, month, day


def create_spark_session():
    spark = (SparkSession.builder
        .appName("StockMarketBatchProcessor")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars", "/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://stock-market-data/iceberg")
        .config("fs.s3a.access.key", "minioadmin")
        .config("fs.s3a.secret.key", "minioadmin")
        .config("fs.s3a.endpoint", "http://minio:9000")
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("fs.s3a.connection.ssl.enabled", "false")
        .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    return spark


def main():
    # ✅ Get date once — used for output path
    year, month, day = get_output_date()

    spark = create_spark_session()

    try:
        input_path  = (
            f"s3a://stock-market-data/raw/historical/"
            f"year={year}/month={month}/day={day}/")
        # ✅ Output path uses same date as {{ ds }} passed from Airflow
        output_path = (
            f"s3a://stock-market-data/processed/historical/"
            f"year={year}/month={month}/day={day}/"
        )

        print(f"\nReading data from S3: {input_path}")
        print(f"Writing to Iceberg table: iceberg.stock_market.historical_stocks")

        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{input_path}"))

        count = df.count()
        if count == 0:
            print("No data found to process")
            sys.exit(0)

        df.show(5)
        df.printSchema()

        processed_df = (df
            .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
            .withColumn("daily_range",
                        F.round(F.col("high") - F.col("low"), 2))
            .withColumn("daily_return_pct",
                        F.round(
                            ((F.col("close") - F.col("open")) / F.col("open")) * 100, 2))
            .withColumn("is_positive_day", F.col("close") > F.col("open"))
            .withColumn("sma_5",  F.round(F.avg("close").over(__window(5)),  2))
            .withColumn("sma_20", F.round(F.avg("close").over(__window(20)), 2))
            .withColumn("processing_time", F.current_timestamp())
        )

        print("\n---- Processing Historical Stock Data")
        print(f"Record count: {processed_df.count()}")
        processed_df.select(
            "symbol", "date", "open", "high", "low",
            "volume", "close", "daily_return_pct", "sma_5", "sma_20"
        ).show(5)

                # Create namespace if it doesn't exist
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.stock_market")

        # Register temp view FIRST
        processed_df.createOrReplaceTempView("processed_df_view")

        # Create Iceberg table schema if it doesn't exist
        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg.stock_market.historical_stocks
            USING iceberg
            PARTITIONED BY (symbol)
            AS SELECT * FROM processed_df_view
            WHERE 1=0
        """)

        # Write data to Iceberg table
        (processed_df
            .writeTo("iceberg.stock_market.historical_stocks")
            .using("iceberg")
            .partitionedBy("symbol")
            .createOrReplace())

        processed_df.select(
            "symbol", "date", "open", "high", "low", "close",
            "volume", "daily_range", "daily_return_pct",
            "is_positive_day", "sma_5", "sma_20"
        ).orderBy("symbol", "date").show(20, truncate=False)

        print(f"\n\nOutput written to Iceberg table: iceberg.stock_market.historical_stocks")
        print("\n" + "=" * 45)
        print("BATCH PROCESSING COMPLETE")
        print("=" * 45)

    except Exception as e:
        print(f"Error in batch processing: {e}")
        sys.exit(1)
    finally:
        spark.stop()


def __window(days: int):
    from pyspark.sql.window import Window
    return (Window
            .partitionBy("symbol")
            .orderBy("date")
            .rowsBetween(-(days - 1), 0))


if __name__ == "__main__":
    main()