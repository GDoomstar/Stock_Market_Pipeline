import json
import logging
import os 
import time 
from datetime import datetime, timedelta
import sys

import pandas as pd 
import yfinance as yf
from confluent_kafka import Producer
from dotenv import load_dotenv
from typing import Optional

#Load Env Variables
load_dotenv()

#Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

logger = logging.getLogger(__name__)

#Kafka Variables
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_BATCH = "stock-market-batch"

#Define stocks to collect for historical data
STOCKS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "INTC",
    "JPM",
    "V"
]

class HistoricalDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_BATCH):
        self.logger = logger
        self.topic = topic

        #Create producer instance
        self.producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "historical-data-collector-0",
        }

        try:
            self.producer = Producer(self.producer_config)
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer {e}")
            raise

    def fetch_historical_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        try:
            self.logger.info(f"Fetching historical data for {symbol} from {start_date} to {end_date}")

            ticker = yf.Ticker(symbol)

            # Get historical data for the date range
            df = ticker.history(start=start_date, end=end_date)

            if df.empty:
                self.logger.warning(f"No data found for {symbol} in the specified date range")
                return None

            df.reset_index(inplace=True)

            df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            }, inplace=True)

            df['date'] = df['date'].dt.strftime("%Y-%m-%d")
            df['symbol'] = symbol

            df = df[['date','symbol',"open","high","low","close","volume"]]

            self.logger.info(f"Successfully fetched {len(df)} days of data for {symbol}")

            return df

        except Exception as e:
            self.logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return None
        
    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {err}")
        else:
            self.logger.info(f"Message delivered successfully to topic {msg.topic()} [{msg.partition()}]")

    def produce_to_kafka(self, df: pd.DataFrame, symbol: str):
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        df['batch_id'] = batch_id
        df['batch_date'] = datetime.now().strftime("%Y-%m-%d")

        records = df.to_dict(orient="records")

        successful_records = 0
        failed_records = 0

        for record in records:
            try:
                data = json.dumps(record)

                self.producer.produce(
                    topic=self.topic,
                    key=symbol,
                    value=data,
                    callback=self.delivery_report
                )

                self.producer.poll(0)
                successful_records += 1

            except Exception as e:
                self.logger.error(f"Failed to produce message for {symbol}: {e}")
                failed_records += 1

        self.producer.flush()
        self.logger.info(f"Successfully produced {successful_records} records for {symbol}, failed: {failed_records}")
    
    def collect_historical_data(self, start_date: str, end_date: str):
        symbols = STOCKS

        self.logger.info(f"Starting historical data collection for {len(symbols)} symbols from {start_date} to {end_date}")

        successful_symbols = 0
        failed_symbols = 0

        for symbol in symbols:
            try:
                # Fetch historical data for the date range
                df = self.fetch_historical_data(symbol, start_date, end_date)

                if df is not None and not df.empty:
                    self.produce_to_kafka(df, symbol)
                    successful_symbols += 1
                else:
                    self.logger.warning(f"No data returned for {symbol}")
                    failed_symbols += 1
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                failed_symbols += 1
            
            time.sleep(1)  # Be nice to the API
        
        self.logger.info(f"Historical data collection completed. Successful: {successful_symbols}, Failed: {failed_symbols}")

def main():
    try:
        # Get date range from command line arguments
        if len(sys.argv) < 2:
            logger.error("Usage: python batch_data_producer.py <start_date> [end_date]")
            logger.error("  start_date: Start date in YYYY-MM-DD format")
            logger.error("  end_date: End date in YYYY-MM-DD format (optional, defaults to today)")
            sys.exit(1)
        
        start_date = sys.argv[1]
        
        if len(sys.argv) >= 3:
            end_date = sys.argv[2]
        else:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Starting Historical Stock Data Collector from {start_date} to {end_date}")

        collector = HistoricalDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_BATCH
        )

        # Collect historical data for the date range
        collector.collect_historical_data(start_date=start_date, end_date=end_date)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()