import logging
import sys
import traceback
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd
import snowflake.connector
import io 
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# S3/MinIO configuration
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
S3_BUCKET = 'stock-market-data'

# Snowflake Config
SNOWFLAKE_ACCOUNT = ''
SNOWFLAKE_USER = ''
SNOWFLAKE_PASSWORD = ''
SNOWFLAKE_DATABASE = ""
SNOWFLAKE_SCHEMA = ""
SNOWFLAKE_WAREHOUSE = ""
SNOWFLAKE_TABLE = ""

def init_s3_client():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            verify=False
        )
        logger.info("S3 client initialized")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        raise

def init_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info("Snowflake connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to establish Snowflake connection: {e}")
        raise

def create_snowflake_table(conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        symbol STRING,
        date DATE,
        daily_open FLOAT,
        daily_high FLOAT,
        daily_low FLOAT,
        daily_volume FLOAT,
        daily_close FLOAT,
        daily_change FLOAT,
        last_updated TIMESTAMP_NTZ,
        PRIMARY KEY (symbol, date)
    )
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Snowflake table created/verified")
    except Exception as e:
        logger.error(f"Failed to create Snowflake table: {e}")
        raise
    finally:
        cursor.close()

def read_5_years_processed_data(s3_client, start_date, end_date):
    """Read 5 years of processed data from S3"""
    logger.info(f"\n------ Reading 5 Years Processed Data: {start_date} to {end_date} ------")
    
    # Use the date range folder structure from Spark processor
    date_range_folder = f"{start_date}_to_{end_date}"
    s3_prefix = f"processed/historical/{date_range_folder}/"
    
    logger.info(f"Reading data from s3://{S3_BUCKET}/{s3_prefix}")

    try:
        # List all objects in the date range folder
        paginator = s3_client.get_paginator('list_objects_v2')
        dfs = []
        total_files = 0

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_prefix):
            if "Contents" not in page:
                logger.warning(f"No data found for date range {start_date} to {end_date}")
                continue
            
            for obj in page['Contents']:
                if obj['Key'].endswith(".parquet"):
                    total_files += 1
                    try:
                        # Extract symbol from path (symbol=SYMBOL/)
                        symbol = None
                        parts = obj['Key'].split("/")
                        for part in parts:
                            if part.startswith("symbol="):
                                symbol = part.split("=")[1]
                                break

                        if not symbol:
                            logger.warning(f"Could not extract symbol from: {obj['Key']}")
                            continue

                        # Read the parquet file
                        response = s3_client.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                        parquet_data = response['Body'].read()
                        parquet_buffer = io.BytesIO(parquet_data)
                        df = pd.read_parquet(parquet_buffer)
                        
                        # Add symbol if not present
                        if "symbol" not in df.columns:
                            df['symbol'] = symbol
                        
                        dfs.append(df)
                        logger.info(f"Read {len(df)} records from {obj['Key']}")
                        
                    except Exception as e:
                        logger.error(f"Failed to read file {obj['Key']}: {e}")
                        continue

        if not dfs:
            logger.error(f"No processed data found in {s3_prefix}")
            return None
        
        # Combine all data
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} total records from {total_files} files")
        
        # Data cleaning and preparation
        if "date" in combined_df.columns:
            combined_df['date'] = pd.to_datetime(combined_df['date']).dt.date
        else:
            logger.error("Missing 'date' column in processed data")
            return None
        
        # Add timestamp and remove duplicates
        combined_df['last_updated'] = datetime.now()
        combined_df = combined_df.drop_duplicates(subset=['symbol', 'date'], keep='last')
        
        # Select required columns
        required_columns = [
            "symbol", "date", "daily_open", "daily_high", "daily_low", 
            "daily_volume", "daily_close", "daily_change", "last_updated"
        ]
        
        # Check for missing columns
        missing_columns = [col for col in required_columns if col not in combined_df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            logger.info(f"Available columns: {combined_df.columns.tolist()}")
            return None
        
        combined_df = combined_df[required_columns]
        
        # Log sample data
        logger.info(f"Sample of loaded data ({len(combined_df)} total records):")
        logger.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
        logger.info(f"Symbols: {combined_df['symbol'].unique().tolist()}")
        
        return combined_df
    
    except Exception as e:
        logger.error(f"Failed to read 5 years data from S3: {e}")
        traceback.print_exc()
        return None

def convert_timestamp_to_string(timestamp_val):
    """Convert timestamp to string format for Snowflake"""
    if pd.isna(timestamp_val):
        return None
    elif isinstance(timestamp_val, (pd.Timestamp, datetime)):
        return timestamp_val.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return str(timestamp_val)

def incremental_load_to_snowflake(conn, df):
    """Perform incremental load of 5 years data to Snowflake"""
    logger.info(f"\n----------- Performing Incremental Load of {len(df)} records into Snowflake -----------")

    if df is None or df.empty:
        logger.info("No data to load")
        return
    
    try:
        cursor = conn.cursor()

        # Create temporary staging table
        stage_table = "TEMP_STAGE_TABLE"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE {SNOWFLAKE_TABLE}")

        # Prepare data for insertion - convert timestamps to strings
        records = []
        columns = ['symbol', 'date', 'daily_open', 'daily_high', 'daily_low', 
                  'daily_volume', 'daily_close', 'daily_change', 'last_updated']
        
        for _, row in df.iterrows():
            record = []
            for col in columns:
                val = row[col]
                
                # Handle different data types for Snowflake compatibility
                if pd.isna(val):
                    record.append(None)
                elif col == 'date':
                    # Convert date to string
                    if isinstance(val, (pd.Timestamp, datetime)):
                        record.append(val.strftime('%Y-%m-%d'))
                    else:
                        record.append(str(val))
                elif col == 'last_updated':
                    # Convert timestamp to string
                    record.append(convert_timestamp_to_string(val))
                elif isinstance(val, (np.floating, float)):
                    record.append(float(val))
                elif isinstance(val, (np.integer, int)):
                    record.append(int(val))
                else:
                    record.append(str(val) if val is not None else None)
            records.append(tuple(record))

        # Insert into temporary table using parameter binding
        placeholders = ",".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {stage_table} ({','.join(columns)}) VALUES ({placeholders})"
        
        logger.info(f"Inserting {len(records)} records into temporary table...")
        
        # Batch insert to avoid memory issues
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            logger.info(f"Inserted batch {i//batch_size + 1}/{(len(records)-1)//batch_size + 1}")
        
        logger.info("Insert into temporary table completed")

        # Merge into main table
        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.symbol = source.symbol AND target.date = source.date
        WHEN MATCHED THEN
            UPDATE SET
                target.daily_open = source.daily_open,
                target.daily_high = source.daily_high,
                target.daily_low = source.daily_low,
                target.daily_volume = source.daily_volume,
                target.daily_close = source.daily_close,
                target.daily_change = source.daily_change,
                target.last_updated = source.last_updated
        WHEN NOT MATCHED THEN
            INSERT (symbol, date, daily_open, daily_high, daily_low, daily_volume, daily_close, daily_change, last_updated)
            VALUES (source.symbol, source.date, source.daily_open, source.daily_high, source.daily_low, 
                    source.daily_volume, source.daily_close, source.daily_change, source.last_updated)
        """

        cursor.execute(merge_query)
        merged_count = cursor.rowcount
        logger.info(f"Successfully merged {merged_count} records into Snowflake")

    except Exception as e:
        logger.error(f"Failed to load data to Snowflake: {e}")
        traceback.print_exc()
        raise
    finally:
        cursor.close()

def main():
    logger.info("\n=========================================")
    logger.info("STARTING SNOWFLAKE 5-YEAR DATA LOAD")
    logger.info("=========================================\n")

    # Get date range from command line arguments
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        logger.info(f"Loading data for range: {start_date} to {end_date}")
    elif len(sys.argv) == 2:
        # If only end date provided, calculate start date (5 years back)
        end_date = sys.argv[1]
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=5*365)
        start_date = start_dt.strftime("%Y-%m-%d")
        logger.info(f"Loading 5 years data ending on {end_date} (from {start_date})")
    else:
        # Default to last 5 years
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_dt = datetime.now() - timedelta(days=5*365)
        start_date = start_dt.strftime("%Y-%m-%d")
        logger.info(f"Loading default 5 years range: {start_date} to {end_date}")

    # Initialize clients
    s3_client = init_s3_client()
    conn = init_snowflake_connection()

    try:
        # Ensure table exists
        create_snowflake_table(conn)

        # Read 5 years of processed data
        df = read_5_years_processed_data(s3_client, start_date, end_date)

        if df is not None and not df.empty:
            logger.info(f"Loaded {len(df)} records for {len(df['symbol'].unique())} symbols")
            logger.info(f"Date range in data: {df['date'].min()} to {df['date'].max()}")
            
            # Load to Snowflake
            incremental_load_to_snowflake(conn, df)
            logger.info("5-year data load to Snowflake completed successfully!")
        else:
            logger.error("No data found to load into Snowflake")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to execute Snowflake load: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()
        logger.info("Snowflake connection closed")

if __name__ == "__main__":
    main()