import os
import sys 
import traceback
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "http://minio:9000"

def create_spark_session():
    print("Initializing Spark Session with S3 Configuration") 
    
    spark = (SparkSession.builder
        .appName("StockMarketBatchProcessor")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        .getOrCreate())
    
    spark_conf = spark.sparkContext._jsc.hadoopConfiguration()
    spark_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    spark_conf.set("fs.s3a.path.style.access", "true")
    spark_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark_conf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized successfully")

    return spark

def read_data_from_s3(spark, start_date=None, end_date=None):
    print("Reading 5 years of data from S3")
    
    if start_date is None:
        # Default to 5 years ago if no start date provided
        start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Reading data from date range: {start_date} to {end_date}")

    # Read from the date range folder structure created by the consumer
    date_range_folder = f"{start_date}_to_{end_date}"
    s3_path = f"s3a://{MINIO_BUCKET}/raw/historical/{date_range_folder}/"
    
    print(f"Reading data from: {s3_path}")

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path + "*.csv")
        print("Sample data:")
        df.show(5, truncate=False)
        df.printSchema()
        print(f"Total records read: {df.count()}")
        return df
    except Exception as e:
        print(f"Error reading data from S3: {str(e)}")
        print("Trying alternative path structure...")
        
        # Fallback: Try reading from the entire historical folder
        try:
            s3_path_fallback = f"s3a://{MINIO_BUCKET}/raw/historical/"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path_fallback + "*.csv")
            print("Successfully read data using fallback path")
            df.show(5, truncate=False)
            print(f"Total records read: {df.count()}")
            return df
        except Exception as e2:
            print(f"Fallback also failed: {str(e2)}")
            return None

def process_stock_data(df):
    print("\n---- Processing Historical Stock Data")

    if df is None or df.count() == 0:
        print("No data to process")
        return None
    
    try:
        record_count = df.count()
        print(f"Record count: {record_count}")

        # Define window for daily metrics
        window_day = Window.partitionBy("symbol", "date").orderBy("date")

        # Calculate metrics - ensure we have the correct column names
        print("Available columns:", df.columns)
        
        # If we have multiple records per symbol per day, aggregate them
        processed_df = df.groupBy("symbol", "date").agg(
            F.first("open").alias("daily_open"),
            F.max("high").alias("daily_high"),
            F.min("low").alias("daily_low"),
            F.sum("volume").alias("daily_volume"),
            F.last("close").alias("daily_close")
        )
        
        # Calculate daily change
        processed_df = processed_df.withColumn(
            "daily_change", 
            ((F.col("daily_close") - F.col("daily_open")) / F.col("daily_open")) * 100
        )

        print("Sample of processed data:")
        processed_df.show(10, truncate=False)
        print(f"Processed {processed_df.count()} daily records")

        return processed_df
    
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        traceback.print_exc()
        return None

def write_to_s3(df, start_date=None, end_date=None):
    print("\n------ Writing processed data to S3")

    if df is None:
        print("No data to write")
        return None
    
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Create output path with date range
    date_range = f"{start_date}_to_{end_date}"
    output_path = f"s3a://{MINIO_BUCKET}/processed/historical/{date_range}/"
    
    print(f"Writing processed data to: {output_path}")

    try:
        # Write partitioned by symbol and date for better organization
        df.write.mode("overwrite").partitionBy("symbol").parquet(output_path)
        print(f"Data successfully written to S3: {output_path}")
        return True
    except Exception as e:
        print(f"Error writing to S3: {str(e)}")
        return False

def main():
    """Main Function to process historical data"""
    print("\n=============================================")
    print("STARTING STOCK MARKET BATCH PROCESSOR - 5 YEARS DATA")
    print("=============================================\n")
    
    # Get date range from command line arguments
    start_date = None
    end_date = None
    
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        print(f"Processing date range: {start_date} to {end_date}")
    elif len(sys.argv) == 2:
        # If only one date provided, assume it's the end date and calculate start date
        end_date = sys.argv[1]
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=5*365)).strftime("%Y-%m-%d")
        print(f"Processing 5 years data ending on {end_date}")
    else:
        # Default to last 5 years
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
        print(f"Processing default 5 years range: {start_date} to {end_date}")

    spark = create_spark_session()

    try:
        # Read 5 years of data
        df = read_data_from_s3(spark, start_date, end_date)

        if df is not None and df.count() > 0:
            processed_df = process_stock_data(df)

            if processed_df is not None:
                success = write_to_s3(processed_df, start_date, end_date)
                if success:
                    print("5 years data processing completed successfully")
                else:
                    print("Error writing processed data to S3")
            else:
                print("Error processing data")
        else:
            print("No data found in S3 for the specified date range")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        traceback.print_exc()
    finally:
        print("\nStopping Spark Session")
        spark.stop()
        print("\n=============================================")
        print("BATCH PROCESSING COMPLETE")
        print("=============================================\n")

if __name__ == "__main__":
    main()