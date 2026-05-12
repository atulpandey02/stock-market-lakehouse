#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Snowflake Stream Analytics Loader
Reads processed realtime parquet from MinIO → merges into Snowflake REALTIME_STOCK

Usage:
  Manual:  python load_stream_to_snowflake.py
  Airflow: python load_stream_to_snowflake.py 2026-03-29  (passes {{ ds }})
"""

import io
import logging
import os
import sys
import traceback
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

SEPARATOR = "=" * 55
MINI_SEP  = "-" * 55

# ── Config — all from env vars ────────────────────────────────────────────────
# ✅ FIX 1: Use env vars not hardcoded credentials
S3_ENDPOINT   = os.getenv('S3_ENDPOINT',      'http://minio:9000')
S3_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
S3_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
S3_BUCKET     = os.getenv('MINIO_BUCKET',     'stock-market-data')

SNOWFLAKE_ACCOUNT   = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER      = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD  = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE  = os.getenv('SNOWFLAKE_STREAM_DATABASE', 'STOCKMARKETSTREAM')
SNOWFLAKE_SCHEMA    = os.getenv('SNOWFLAKE_SCHEMA',    'PUBLIC')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')

# ✅ FIX 2: Use REALTIME_STOCK not DAILY_STREAM_METRICS
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_STREAM_TABLE', 'REALTIME_STOCK')


def get_process_date() -> tuple:
    """
    Returns (year, month, day) as strings.
    - Airflow: reads sys.argv[1] = {{ ds }} (UTC)
    - Manual:  uses datetime.now(UTC)
    """
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        logger.info(f"  Date Source : Airflow ds = {date_str}")
        year, month, day = date_str.split("-")
    else:
        now  = datetime.now(timezone.utc)
        year  = str(now.year)
        month = f"{now.month:02d}"
        day   = f"{now.day:02d}"
        logger.info(f"  Date Source : UTC now = {year}-{month}-{day}")
    return year, month, day


def init_s3_client():
    try:
        client = boto3.client(
            's3',
            endpoint_url          = S3_ENDPOINT,
            aws_access_key_id     = S3_ACCESS_KEY,
            aws_secret_access_key = S3_SECRET_KEY
        )
        logger.info(f"  S3 client initialized → {S3_ENDPOINT}")
        return client
    except Exception as e:
        logger.error(f"  Failed to init S3 client: {e}")
        raise


def init_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user      = SNOWFLAKE_USER,
            password  = SNOWFLAKE_PASSWORD,
            account   = SNOWFLAKE_ACCOUNT,
            warehouse = SNOWFLAKE_WAREHOUSE,
            database  = SNOWFLAKE_DATABASE,
            schema    = SNOWFLAKE_SCHEMA
        )
        logger.info("  Snowflake connection established ✓")
        return conn
    except Exception as e:
        logger.error(f"  Failed to connect Snowflake: {e}")
        raise


def create_snowflake_table(conn):
    """
    Create REALTIME_STOCK table if not exists.
    Column names match current spark_stream_batch_processor.py output:
      ma_15m, ma_1h (15-min and 1-hour window moving averages)
      volatility_15m, volatility_1h
      volume_sum_15m, volume_sum_1h
      window_15m_end, window_1h_end
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS
        {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            symbol          STRING    NOT NULL,
            window_start    TIMESTAMP NOT NULL,
            window_15m_end  TIMESTAMP,
            window_1h_end   TIMESTAMP,
            ma_15m          FLOAT,
            ma_1h           FLOAT,
            volatility_15m  FLOAT,
            volatility_1h   FLOAT,
            volume_sum_15m  BIGINT,
            volume_sum_1h   BIGINT,
            batch_id        STRING,
            ingestion_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (symbol, window_start)
        )
    """
    try:
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        logger.info(f"  Table ready: {SNOWFLAKE_TABLE} ✓")
    except Exception as e:
        logger.error(f"  Failed to create table: {e}")
        raise
    finally:
        cur.close()


def read_analytics_data(s3_client, year: str, month: str, day: str):
    """
    Read processed realtime data from Iceberg table in MinIO.
    Iceberg stores data as Parquet files under:
    iceberg/stock_market/realtime_stocks/data/
    """
    logger.info(MINI_SEP)
    logger.info("  READING PROCESSED REALTIME DATA FROM ICEBERG (MinIO)")
    logger.info(MINI_SEP)

    s3_prefix = "iceberg/stock_market/realtime_stocks/data/"
    logger.info(f"  Path   : s3://{S3_BUCKET}/{s3_prefix}")
    logger.info(f"  Filter : date = {year}-{month}-{day}")

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)

        if "Contents" not in response:
            logger.warning(f"  No data found at {s3_prefix}")
            return None

        parquet_files = [
            obj for obj in response['Contents']
            if obj['Key'].endswith('.parquet') and obj['Size'] > 0
        ]
        logger.info(f"  Files  : {len(parquet_files)} parquet files found")

        if not parquet_files:
            logger.warning("  No parquet files found in Iceberg data directory")
            return None

        dfs = []
        target_date = f"{year}-{month}-{day}"

        for obj in parquet_files:
            key = obj['Key']
            try:
                symbol = None
                for part in key.split("/"):
                    if part.startswith("symbol="):
                        symbol = part.split("=")[-1]
                        break

                resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                df   = pd.read_parquet(io.BytesIO(resp['Body'].read()))

                if "symbol" not in df.columns and symbol:
                    df['symbol'] = symbol

                # Filter to today's data only using window_start date
                if 'window_start' in df.columns:
                    df['window_start'] = pd.to_datetime(df['window_start'])
                    today = pd.to_datetime(target_date).date()
                    df = df[df['window_start'].dt.date == today]

                if len(df) > 0:
                    dfs.append(df)
                    logger.info(f"  ✓ {symbol}: {len(df)} rows for {target_date}")

            except Exception as e:
                logger.error(f"  Failed to read {key}: {e}")
                continue

        if not dfs:
            logger.warning(f"  No data found for date = {target_date}")
            return None

        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"  Total  : {len(combined)} rows across all symbols")

        # Add batch_id if missing
        if 'batch_id' not in combined.columns:
            combined['batch_id'] = f"batch_{year}{month}{day}"

        # Fix column names — match Iceberg table schema
        for col in ['window_start', 'window_15m_end', 'window_1h_end']:
            if col in combined.columns:
                combined[col] = pd.to_datetime(combined[col])

        if 'symbol' in combined.columns:
            combined['symbol'] = combined['symbol'].astype(str)

        numeric_cols = combined.select_dtypes(include=[np.number]).columns
        combined[numeric_cols] = combined[numeric_cols].fillna(0)

        combined = combined.drop_duplicates(
            subset=['symbol', 'window_start'], keep='last'
        )

        logger.info(f"  Final  : {len(combined)} rows after dedup")
        logger.info(f"  Columns: {combined.columns.tolist()}")

        return combined

    except Exception as e:
        logger.error(f"  Failed to read from Iceberg: {e}")
        logger.error(traceback.format_exc())
        return None


def load_to_snowflake(conn, df: pd.DataFrame):
    """
    MERGE realtime analytics into Snowflake REALTIME_STOCK table.
    Column names match current Spark output: ma_15m, ma_1h etc.
    """
    logger.info(MINI_SEP)
    logger.info("  INCREMENTAL LOAD → SNOWFLAKE")
    logger.info(MINI_SEP)
    logger.info(f"  Target : {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
    logger.info(f"  Rows   : {len(df)}")

    if df is None or df.empty:
        logger.warning("  No data to load")
        return

    cur = conn.cursor()
    try:
        stage = "TEMP_STAGE_REALTIME"
        cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage} LIKE {SNOWFLAKE_TABLE}")

        # ✅ Matches current spark_stream_batch_processor.py column names
        snowflake_cols = [
            'symbol', 'window_start', 'window_15m_end', 'window_1h_end',
            'ma_15m', 'ma_1h', 'volatility_15m', 'volatility_1h',
            'volume_sum_15m', 'volume_sum_1h', 'batch_id'
        ]
        available = [c for c in snowflake_cols if c in df.columns]
        df_load   = df[available]

        records = []
        for _, row in df_load.iterrows():
            record = []
            for val in row:
                if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                    record.append(None)
                elif isinstance(val, (np.floating, float)):
                    record.append(float(val))
                elif isinstance(val, (np.integer, int)):
                    record.append(int(val))
                elif isinstance(val, pd.Timestamp):
                    record.append(val.to_pydatetime())
                else:
                    record.append(val)
            records.append(tuple(record))

        placeholders = ", ".join(["%s"] * len(available))
        insert_sql   = f"INSERT INTO {stage} ({', '.join(available)}) VALUES ({placeholders})"
        cur.executemany(insert_sql, records)
        logger.info(f"  Staged {len(records)} records ✓")

        # Build SET clause for UPDATE
        update_cols = [c for c in available if c not in ('symbol', 'window_start')]
        set_clause  = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
        insert_cols = ", ".join(available)
        insert_vals = ", ".join([f"source.{c}" for c in available])

        merge_sql = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage} AS source
            ON target.symbol       = source.symbol
           AND target.window_start = source.window_start
        WHEN MATCHED THEN
            UPDATE SET {set_clause},
                       target.ingestion_time = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """
        cur.execute(merge_sql)
        conn.commit()
        logger.info("  MERGE complete ✓")

        # Count total rows
        cur.execute(
            f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
        )
        total = cur.fetchone()[0]
        logger.info(f"  Total rows in {SNOWFLAKE_TABLE}: {total}")

    except Exception as e:
        logger.error(f"  Load failed: {e}")
        logger.error(traceback.format_exc())
        conn.rollback()
        raise
    finally:
        cur.close()


def main():
    logger.info(SEPARATOR)
    logger.info("  SNOWFLAKE STREAM ANALYTICS LOAD")
    logger.info(SEPARATOR)

    year, month, day = get_process_date()
    logger.info(f"  Processing date : {year}-{month}-{day}")
    logger.info(f"  MinIO endpoint  : {S3_ENDPOINT}")
    logger.info(f"  Snowflake table : {SNOWFLAKE_TABLE}")
    logger.info(SEPARATOR)

    s3_client = init_s3_client()
    conn      = init_snowflake_connection()

    try:
        create_snowflake_table(conn)

        df = read_analytics_data(s3_client, year, month, day)

        if df is not None and not df.empty:
            load_to_snowflake(conn, df)
            logger.info(SEPARATOR)
            logger.info("  ✅ STREAM SNOWFLAKE LOAD COMPLETE")
            logger.info(SEPARATOR)
        else:
            logger.warning("  No data found — nothing loaded")

    except Exception as e:
        logger.error(f"  Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        conn.close()
        logger.info("  Snowflake connection closed ✓")


if __name__ == "__main__":
    main()