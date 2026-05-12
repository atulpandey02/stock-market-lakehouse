"""
Stock Market Batch Pipeline DAG
=================================
Runs INSIDE Docker (Airflow worker):
  1. fetch_historical_data          Finnhub API → Kafka
  2. consume_historical_data        Kafka → MinIO raw/historical/
  3. wait_for_raw_data              MinIO sensor (actual UTC date)
  4. process_data                   Spark → MinIO processed/historical/
  5. load_historical_to_snowflake   MinIO → Snowflake MERGE
  6. pipeline_complete              Summary log

Runs MANUALLY on Mac terminal after DAG completes:
  dbt run / dbt test                src/dbt/
  python rag_pipeline.py            src/rag/
  streamlit run app.py              src/rag/

Schedule: 9AM weekdays (Mon-Fri)
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


# ── MinIO Sensor ──────────────────────────────────────────────────────────────

class MinIODataSensor(BaseSensorOperator):
    """
    Waits until MinIO has at least `min_files` in today's partition.

    ✅ Uses datetime.now(UTC) — NOT context['ds']
    Reason: consumer writes files using actual UTC time.
            context['ds'] is the logical schedule date which
            lags behind when you manually trigger the DAG.
    """

    @apply_defaults
    def __init__(self, bucket_name, prefix, min_files=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix      = prefix
        self.min_files   = min_files

    def poke(self, context):
        try:
            from minio import Minio
            from datetime import datetime, timezone

            client = Minio(
                'minio:9000',
                access_key='minioadmin',
                secret_key='minioadmin',
                secure=False
            )

            # ✅ Use actual UTC today — matches consumer file paths
            now   = datetime.now(timezone.utc)
            year  = str(now.year)
            month = f"{now.month:02d}"
            day   = f"{now.day:02d}"

            path = (
                f"{self.prefix}/"
                f"year={year}/month={month}/day={day}"
            )

            self.log.info(f"Checking MinIO path: {path} (actual UTC today)")

            objects    = list(client.list_objects(self.bucket_name, prefix=path, recursive=True))
            data_files = [o for o in objects if o.size > 0]

            self.log.info(f"Found {len(data_files)} files (need {self.min_files}) in {path}")
            return len(data_files) >= self.min_files

        except Exception as e:
            self.log.error(f"MinIO sensor error: {e}")
            return False


# ── Helpers ───────────────────────────────────────────────────────────────────

SCRIPTS = "/opt/airflow/dags/scripts"


def run_script(script_name: str, *args):
    """Run a Python script from the scripts folder, raise on failure."""
    cmd    = [sys.executable, f"{SCRIPTS}/{script_name}"] + list(args)
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        raise Exception(f"{script_name} failed with exit code {result.returncode}")


# ── Task 1: Fetch Historical Data ─────────────────────────────────────────────

def fetch_historical_data(**context):
    """
    Fetch OHLCV data for 10 stocks from Finnhub API.
    Produces each row as a Kafka message → stock-market-batch topic.
    Stocks: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, INTC, JPM, V
    """
    print("=" * 55)
    print("  TASK 1: Fetching historical data from Finnhub")
    print("=" * 55)
    run_script("batch_data_producer.py")
    print("✓ Historical data produced to Kafka")


# ── Task 2: Consume Historical Data ──────────────────────────────────────────

def consume_historical_data(**context):
    """
    Consume messages from Kafka stock-market-batch topic.
    Writes CSV files to MinIO: raw/historical/year=/month=/day=/
    Exits automatically when Kafka topic is drained.
    """
    print("=" * 55)
    print("  TASK 2: Consuming Kafka → MinIO")
    print("=" * 55)
    run_script("batch_data_consumer.py")
    print("✓ Data written to MinIO raw/historical/")


# ── Task 4: Spark Batch Processing ───────────────────────────────────────────

def process_data(**context):
    """
    Run Spark batch processor via docker exec on spark-client container.
    Reads all CSVs from raw/historical/ recursively.
    Computes: SMA-5, SMA-20, daily_return_pct, daily_range, is_positive_day.
    Writes parquet to: processed/historical/year=/month=/day=/symbol=/
    """
    print("=" * 55)
    print("  TASK 4: Running Spark batch processor")
    print("=" * 55)

    cmd = [
        "docker", "exec",
        "stockmarketdatapipeline_v2-spark-master-1",
        "/opt/spark/bin/spark-submit",
        "--master",          "spark://spark-master:7077",
        "--conf",            "spark.jars.ivy=/tmp/.ivy2",
        "--driver-memory",   "512m",
        "--executor-memory", "512m",
        "--executor-cores",  "1",
        "/opt/spark/jobs/spark_batch_processor.py",
    ]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True , text = True)

    # Print everything Spark said
    if result.stdout:
        print("SPARK STDOUT:")
        print(result.stdout)
    if result.stderr:
        print("SPARK STDERR:")
        print(result.stderr)

    if result.returncode == 0:
        print("✓ Spark batch processing complete")
    else:
        raise Exception(
            f"Spark failed with exit code: {result.returncode}"
        )


# ── Task 5: Load to Snowflake ─────────────────────────────────────────────────

def load_historical_to_snowflake(**context):
    """
    Read processed parquet from MinIO and MERGE into Snowflake.
    Uses MERGE (not INSERT) — DAG reruns are idempotent.
    Primary key: (symbol, date)
    Target: STOCKMARKETBATCH.PUBLIC.HISTORICAL_STOCK
    """
    print("=" * 55)
    print("  TASK 5: Loading to Snowflake")
    print("=" * 55)

    # Use actual UTC today — matches where Spark wrote the parquet files
    now   = datetime.now(timezone.utc)
    ds    = now.strftime("%Y-%m-%d")
    print(f"  Loading date: {ds}")

    run_script("load_to_snowflake.py", ds)
    print("✓ Snowflake MERGE complete")


# ── Task 6: Pipeline Complete ─────────────────────────────────────────────────

def pipeline_complete(**context):
    """
    Print a summary and remind what to run manually on Mac terminal next.
    dbt and RAG are NOT in this DAG because they are not installed
    inside the Airflow Docker container.
    """
    now = datetime.now(timezone.utc)
    print("=" * 55)
    print("  AIRFLOW PIPELINE COMPLETE ✅")
    print("=" * 55)
    print(f"  Date: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    print("  Tasks completed inside Docker:")
    print("    ✅ fetch_historical_data")
    print("    ✅ consume_historical_data")
    print("    ✅ wait_for_raw_data (sensor)")
    print("    ✅ process_data (Spark)")
    print("    ✅ load_historical_to_snowflake")
    print()
    print("  Now run these on your Mac terminal:")
    print("    cd src/dbt")
    print("    dbt run --profiles-dir . --project-dir .")
    print("    dbt test --profiles-dir . --project-dir .")
    print()
    print("    cd src/rag")
    print("    python rag_pipeline.py")
    print("    streamlit run app.py")
    print("=" * 55)


# ── DAG Definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "Atul",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_batch_pipeline",
    default_args=default_args,
    description="Finnhub → Kafka → MinIO → Spark → Snowflake",
    schedule_interval="0 9 * * 1-5",   # 9AM Mon-Fri
    start_date=datetime(2025, 9, 15),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "historical", "snowflake"],
) as dag:

    t1_fetch = PythonOperator(
        task_id="fetch_historical_data",
        python_callable=fetch_historical_data,
        execution_timeout=timedelta(minutes=30),
    )

    t2_consume = PythonOperator(
        task_id="consume_historical_data",
        python_callable=consume_historical_data,
        execution_timeout=timedelta(minutes=20),
    )

    # ✅ Sensor uses actual UTC today, not context['ds']
    t3_sensor = MinIODataSensor(
        task_id="wait_for_raw_data",
        bucket_name="stock-market-data",
        prefix="raw/historical",
        min_files=10,
        poke_interval=30,
        timeout=600,
    )

    t4_spark = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        execution_timeout=timedelta(minutes=20),
    )

    t5_snowflake = PythonOperator(
        task_id="load_historical_to_snowflake",
        python_callable=load_historical_to_snowflake,
        execution_timeout=timedelta(minutes=15),
    )

    t6_done = PythonOperator(
        task_id="pipeline_complete",
        python_callable=pipeline_complete,
    )

    # ── Flow ──────────────────────────────────────────────────────────────────
    (
        t1_fetch
        >> t2_consume
        >> t3_sensor
        >> t4_spark
        >> t5_snowflake
        >> t6_done
    )