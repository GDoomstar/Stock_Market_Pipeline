import json 
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd 
import numpy as np

from confluent_kafka import Consumer, KafkaError
from minio import Minio
from minio.error import S3Error

from dotenv import load_dotenv

load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_BATCH = "stock-market-batch"
KAFKA_GROUP_ID = "stock-market-batch-consumer-group"

# MinIO configuration
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "minio:9000"

class BatchDataConsumer:
    def __init__(self, start_date=None, end_date=None):
        self.minio_client = self.create_minio_client()
        self.ensure_bucket_exists()
        
        # Store date range for processing
        self.start_date = start_date
        self.end_date = end_date
        
        # Kafka consumer configuration
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([KAFKA_TOPIC_BATCH])
        
        logger.info(f"BatchDataConsumer initialized for date range: {start_date} to {end_date}")

    def create_minio_client(self):
        """Initialize MinIO Client."""
        return Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

    def ensure_bucket_exists(self):
        try:
            if not self.minio_client.bucket_exists(MINIO_BUCKET):
                self.minio_client.make_bucket(MINIO_BUCKET)
                logger.info(f"Created bucket {MINIO_BUCKET}")
            else:
                logger.info(f"Bucket {MINIO_BUCKET} already exists")
        except S3Error as e:
            logger.error(f"Error creating bucket {MINIO_BUCKET}: {e}")
            raise

    def process_messages_batch(self, batch_size=100, timeout_seconds=300):
        """
        Process messages in batches for efficiency
        """
        logger.info(f"Starting batch consumption for date range: {self.start_date} to {self.end_date}")
        
        batch_data = defaultdict(list)
        processed_count = 0
        no_message_count = 0
        max_no_messages = 30  # Break after 30 seconds of no messages
        
        try:
            while no_message_count < max_no_messages:
                msg = self.consumer.poll(timeout=1.0)  # 1 second timeout
                
                if msg is None:
                    no_message_count += 1
                    # If we have some data but no new messages, process what we have
                    if batch_data and no_message_count >= 5:  # Wait 5 seconds for more messages
                        self.process_batch(batch_data)
                        self.consumer.commit()
                        batch_data = defaultdict(list)
                        logger.info(f"Processed batch after {no_message_count} seconds of no new messages")
                    continue
                
                # Reset no message counter
                no_message_count = 0
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Process message
                    data = json.loads(msg.value().decode("utf-8"))
                    symbol = data['symbol']
                    
                    # Add to batch
                    batch_data[symbol].append(data)
                    processed_count += 1
                    
                    # Process batch when we reach batch_size
                    if processed_count % batch_size == 0:
                        logger.info(f"Processed {processed_count} messages, writing batch...")
                        self.process_batch(batch_data)
                        self.consumer.commit()
                        batch_data = defaultdict(list)
                        logger.info(f"Committed offsets after {processed_count} messages")
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
            
            # Process any remaining messages in the batch
            if batch_data:
                remaining_count = sum(len(v) for v in batch_data.values())
                logger.info(f"Processing final batch with {remaining_count} messages")
                self.process_batch(batch_data)
                self.consumer.commit()
            
            logger.info(f"Batch consumption completed. Total messages processed: {processed_count}")
            
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error in batch processing: {e}")
        finally:
            self.consumer.close()
            logger.info("Consumer closed")

    def process_batch(self, batch_data):
        """
        Process a batch of messages grouped by symbol
        """
        for symbol, records in batch_data.items():
            try:
                if records:
                    self.write_symbol_data(symbol, records)
                    logger.info(f"Successfully processed {len(records)} records for {symbol}")
            except Exception as e:
                logger.error(f"Failed to process batch for {symbol}: {e}")

    def write_symbol_data(self, symbol, records):
        """
        Write all records for a symbol to a single CSV file
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Use the date range in the filename for clarity
            date_range = f"{self.start_date}_to_{self.end_date}"
            timestamp = datetime.now().strftime('%H%M%S')
            
            # Create object name with date range
            object_name = f"raw/historical/{date_range}/{symbol}_{timestamp}.csv"
            
            # Create temporary file
            temp_file = f"/tmp/{symbol}_{timestamp}.csv"
            df.to_csv(temp_file, index=False)
            
            # Upload to MinIO
            self.minio_client.fput_object(
                MINIO_BUCKET,
                object_name,
                temp_file,
            )
            logger.info(f"Wrote {len(records)} records for {symbol} to s3://{MINIO_BUCKET}/{object_name}")
            
            # Clean up temporary file
            os.remove(temp_file)
            
        except Exception as e:
            logger.error(f"Failed to write data for {symbol}: {e}")
            raise

def main():
    import sys
    
    if len(sys.argv) < 2:
        logger.error("Usage: python batch_data_consumer.py <start_date> [end_date] [batch_size] [timeout]")
        logger.error("  start_date: Start date in YYYY-MM-DD format")
        logger.error("  end_date: End date in YYYY-MM-DD format (optional, defaults to today)")
        logger.error("  batch_size: Batch size (optional, defaults to 200)")
        logger.error("  timeout: Timeout in seconds (optional, defaults to 600)")
        sys.exit(1)
    
    start_date = sys.argv[1]
    
    # Parse optional arguments
    end_date = datetime.now().strftime('%Y-%m-%d')
    batch_size = 200
    timeout = 600
    
    if len(sys.argv) >= 3:
        end_date = sys.argv[2]
    
    if len(sys.argv) >= 4:
        batch_size = int(sys.argv[3])
    
    if len(sys.argv) >= 5:
        timeout = int(sys.argv[4])
    
    logger.info(f"Starting Batch Data Consumer for date range: {start_date} to {end_date}")
    logger.info(f"Batch size: {batch_size}, Timeout: {timeout} seconds")
    
    try:
        consumer = BatchDataConsumer(start_date=start_date, end_date=end_date)
        consumer.process_messages_batch(
            batch_size=batch_size,
            timeout_seconds=timeout
        )
        
        logger.info("Batch data consumption completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in batch data consumer: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())