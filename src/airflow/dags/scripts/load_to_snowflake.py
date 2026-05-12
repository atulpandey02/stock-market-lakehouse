"""
Snowflake Incremental Load — Historical Stock Data
Reads processed parquet files from MinIO → merges into Snowflake

Usage:
  Manual:  python load_to_snowflake.py
  Airflow: python load_to_snowflake.py 2026-03-29  (passes {{ ds }})
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

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

SEPARATOR = "=" * 55
MINI_SEP  = "-" * 55

# ── Config — all from env vars ────────────────────────────────────────────────
# ✅ FIX: use env vars not hardcoded values
S3_ENDPOINT   = os.getenv('S3_ENDPOINT',   'http://minio:9000')  # minio:9000 in Docker
S3_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
S3_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
S3_BUCKET     = os.getenv('MINIO_BUCKET',  'stock-market-data')

SNOWFLAKE_ACCOUNT   = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER      = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD  = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE  = os.getenv('SNOWFLAKE_DATABASE',  'STOCKMARKETBATCH')
SNOWFLAKE_SCHEMA    = os.getenv('SNOWFLAKE_SCHEMA',    'PUBLIC')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
SNOWFLAKE_TABLE     = os.getenv('SNOWFLAKE_TABLE',     'HISTORICAL_STOCK')


def get_process_date() -> tuple:
    """
    Returns (year, month, day) as strings.
    - Airflow: reads sys.argv[1] = {{ ds }} e.g. "2026-03-29"
    - Manual:  uses today in UTC
    """
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        logger.info(f"  Date Source : Airflow ds = {date_str}")
        year, month, day = date_str.split("-")
    else:
        now  = datetime.now(timezone.utc)
        year, month, day = str(now.year), f"{now.month:02d}", f"{now.day:02d}"
        logger.info(f"  Date Source : Manual UTC = {year}-{month}-{day}")
    return year, month, day


# ── S3 Client ─────────────────────────────────────────────────────────────────

def init_s3_client():
    try:
        client = boto3.client(
            's3',
            endpoint_url         = S3_ENDPOINT,
            aws_access_key_id    = S3_ACCESS_KEY,
            aws_secret_access_key= S3_SECRET_KEY
        )
        logger.info(f"  S3 client initialized → {S3_ENDPOINT}")
        return client
    except Exception as e:
        logger.error(f"  Failed to init S3 client: {e}")
        raise


# ── Snowflake Connection ──────────────────────────────────────────────────────

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
        logger.error(f"  Failed to connect to Snowflake: {e}")
        raise


# ── Create Table ──────────────────────────────────────────────────────────────

def create_snowflake_table(conn):
    """
    Create HISTORICAL_STOCK table if not exists.
    Includes ALL columns output by spark_batch_processor.py:
      open, high, low, close, volume (from Alpha Vantage)
      daily_range, daily_return_pct, is_positive_day (computed by Spark)
      sma_5, sma_20 (moving averages from Spark)
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS
        {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            symbol           STRING    NOT NULL,
            date             DATE      NOT NULL,
            open_price       FLOAT,
            high_price       FLOAT,
            low_price        FLOAT,
            close_price      FLOAT,
            volume           BIGINT,
            daily_range      FLOAT,
            daily_return_pct FLOAT,
            is_positive_day  BOOLEAN,
            sma_5            FLOAT,
            sma_20           FLOAT,
            batch_date       DATE,
            last_updated     TIMESTAMP,
            PRIMARY KEY (symbol, date)
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


# ── Read Parquet from MinIO ───────────────────────────────────────────────────

def read_processed_data(s3_client, year: str, month: str, day: str):
    """
    Read processed data from Iceberg table in MinIO.
    Iceberg stores data as Parquet files under:
    iceberg/stock_market/historical_stocks/data/
    We filter by batch_date to load only today's records.
    """
    logger.info(MINI_SEP)
    logger.info("  READING PROCESSED DATA FROM ICEBERG (MinIO)")
    logger.info(MINI_SEP)

    # Iceberg data path — HadoopCatalog stores data here
    s3_prefix = "iceberg/stock_market/historical_stocks/data/"
    logger.info(f"  Path   : s3://{S3_BUCKET}/{s3_prefix}")
    logger.info(f"  Filter : batch_date = {year}-{month}-{day}")

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=s3_prefix
        )

        if "Contents" not in response:
            logger.warning(f"  No files found at {s3_prefix}")
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
                # Extract symbol from Iceberg partition path e.g. symbol=AAPL
                symbol = None
                for part in key.split("/"):
                    if part.startswith("symbol="):
                        symbol = part.split("=")[-1]
                        break

                response_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                df = pd.read_parquet(io.BytesIO(response_obj['Body'].read()))

                if "symbol" not in df.columns and symbol:
                    df['symbol'] = symbol

                # Filter to today's batch only
                if 'batch_date' in df.columns:
                    df['batch_date'] = pd.to_datetime(df['batch_date']).dt.date
                    today = pd.to_datetime(target_date).date()
                    df = df[df['batch_date'] == today]

                if len(df) > 0:
                    dfs.append(df)
                    logger.info(f"  ✓ {symbol}: {len(df)} rows for {target_date}")

            except Exception as e:
                logger.error(f"  Failed to read {key}: {e}")
                continue

        if not dfs:
            logger.warning(f"  No data found for batch_date = {target_date}")
            return None

        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"  Total  : {len(combined)} rows across all symbols")

        # Column mapping — Spark → Snowflake
        column_map = {
            'open':             'open_price',
            'high':             'high_price',
            'low':              'low_price',
            'close':            'close_price',
            'volume':           'volume',
            'daily_range':      'daily_range',
            'daily_return_pct': 'daily_return_pct',
            'is_positive_day':  'is_positive_day',
            'sma_5':            'sma_5',
            'sma_20':           'sma_20',
            'batch_date':       'batch_date',
        }
        combined = combined.rename(columns=column_map)

        # Type casting
        if 'date' in combined.columns:
            combined['date'] = pd.to_datetime(combined['date']).dt.date
        if 'batch_date' in combined.columns:
            combined['batch_date'] = pd.to_datetime(
                combined['batch_date'].astype(str)
            ).dt.date

        combined['last_updated'] = datetime.now(timezone.utc)
        combined = combined.drop_duplicates(
            subset=['symbol', 'date'], keep='last'
        )

        snowflake_cols = [
            'symbol', 'date', 'open_price', 'high_price', 'low_price',
            'close_price', 'volume', 'daily_range', 'daily_return_pct',
            'is_positive_day', 'sma_5', 'sma_20', 'batch_date', 'last_updated'
        ]
        available = [c for c in snowflake_cols if c in combined.columns]
        combined  = combined[available]

        logger.info(f"  Final  : {len(combined)} rows, {len(available)} columns")
        for _, row in combined.head(3).iterrows():
            logger.info(
                f"    {row['symbol']} | {row['date']} | "
                f"close={row.get('close_price', 'N/A')} | "
                f"sma_5={row.get('sma_5', 'N/A')}"
            )

        return combined

    except Exception as e:
        logger.error(f"  Failed to read from Iceberg: {e}")
        logger.error(traceback.format_exc())
        return None


# ── Incremental Load ──────────────────────────────────────────────────────────

def incremental_load_to_snowflake(conn, df: pd.DataFrame):
    """
    MERGE processed data into Snowflake using temp staging table.
    - Existing rows: UPDATE all metric columns
    - New rows: INSERT
    Primary key: (symbol, date)
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
        stage = "TEMP_STAGE_HISTORICAL"

        # Create temp staging table matching target schema
        cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage} LIKE {SNOWFLAKE_TABLE}")

        # Prepare records
        columns      = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql   = f"INSERT INTO {stage} ({', '.join(columns)}) VALUES ({placeholders})"

        records = []
        for _, row in df.iterrows():
            record = []
            for val in row:
                if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                    record.append(None)
                elif isinstance(val, (np.floating, float)):
                    record.append(float(val))
                elif isinstance(val, (np.integer, int)):
                    record.append(int(val))
                elif isinstance(val, (np.bool_, bool)):
                    record.append(bool(val))
                elif isinstance(val, pd.Timestamp):
                    record.append(val.to_pydatetime())
                else:
                    record.append(val)
            records.append(tuple(record))

        cur.executemany(insert_sql, records)
        logger.info(f"  Staged {len(records)} records ✓")

        # Build dynamic SET clause for UPDATE
        update_cols = [
            c for c in columns
            if c not in ('symbol', 'date')
        ]
        set_clause = ", ".join(
            [f"target.{c} = source.{c}" for c in update_cols]
        )
        insert_cols   = ", ".join(columns)
        insert_values = ", ".join([f"source.{c}" for c in columns])

        merge_sql = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage} AS source
            ON target.symbol = source.symbol
           AND target.date   = source.date
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})
        """

        cur.execute(merge_sql)
        conn.commit()
        logger.info("  MERGE complete ✓")

    except Exception as e:
        logger.error(f"  Load failed: {e}")
        logger.error(traceback.format_exc())
        conn.rollback()
        raise
    finally:
        cur.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info(SEPARATOR)
    logger.info("  SNOWFLAKE INCREMENTAL LOAD — HISTORICAL STOCK")
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

        df = read_processed_data(s3_client, year, month, day)

        if df is not None and not df.empty:
            incremental_load_to_snowflake(conn, df)
            logger.info(SEPARATOR)
            logger.info("  ✅ SNOWFLAKE LOAD COMPLETE")
            logger.info(SEPARATOR)
        else:
            logger.warning("  No data found — nothing loaded to Snowflake")

    except Exception as e:
        logger.error(f"  Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        conn.close()
        logger.info("  Snowflake connection closed ✓")


if __name__ == "__main__":
    main()