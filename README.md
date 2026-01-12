# 📈 End-to-End Real-Time Data Pipeline for Stock Market Analytics

A scalable, containerized, and enterprise-grade real-time data pipeline that processes both historical and live stock market data using modern big data technologies such as **Apache Kafka, Apache Spark, Apache Airflow, MinIO, and Snowflake**.  
This project demonstrates how batch and stream processing can be unified into a single robust architecture for financial analytics.

---

## 🚀 Project Overview

This system builds a complete data engineering pipeline that:

- Ingests historical and real-time stock market data  
- Processes data using **Apache Spark (Batch + Structured Streaming)**  
- Orchestrates workflows using **Apache Airflow**  
- Stores raw and processed data in **MinIO (S3-compatible Data Lake)**  
- Loads curated data into **Snowflake** for analytics  
- Visualizes insights using **Power BI** and **Apache Superset**

Used stock symbols:
AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, INTC, JPM, V

## 🏗️ Architecture

<img width="1238" height="497" alt="StockMarketDataPipeline" src="https://github.com/user-attachments/assets/5057b2a8-c4f6-44a2-8098-cae4c1940646" />

## 🛠️ Tech Stack

| Layer               | Technology                              |
|--------------------|------------------------------------------|
| Data Ingestion      | Apache Kafka                             |
| Processing          | Apache Spark (Batch + Structured Streaming) |
| Orchestration       | Apache Airflow                           |
| Storage (Data Lake) | MinIO (S3 Compatible)                    |
| Data Warehouse      | Snowflake                                |
| Visualization       | Power BI, Apache Superset                |
| Containerization    | Docker, Docker Compose                   |
| Language            | Python, PySpark                          |
| Database (Metadata) | PostgreSQL                               |

---

## ✨ Key Features

- 🔁 Real-time streaming pipeline with Kafka + Spark  
- 📊 Batch processing of 5 years of historical data  
- 📦 Data Lake architecture with **Bronze / Silver / Gold layers**  
- 🧩 Partitioned storage by `year/month/day/hour`  
- 🧠 Financial calculations:
  - OHLC (Open, High, Low, Close)
  - Volume Aggregation
  - Moving Averages
  - Volatility Indicators  
- 🔄 Automated scheduling & retries using Airflow  
- 🐳 Fully containerized deployment  
- 📈 Interactive dashboards in Power BI & Superset  

---
## ⚙️ Setup Instructions

### 1. Prerequisites

- Docker Engine 20+  
- Docker Compose  
- Minimum System:
  - 8 GB RAM (16 GB recommended)
  - 4 CPU cores
  - 50 GB disk space  
- Python 3.9+  
- Snowflake Account  

---

### 2. Clone the Repository

```bash
git clone https://github.com/your-username/stock-market-data-pipeline.git
cd stock-market-data-pipeline
```
---
## ⚙️ 3. Configure Environment Variables

Create a `.env` file in the project root directory:

```env
SNOWFLAKE_ACCOUNT=xxxx
SNOWFLAKE_USER=xxxx
SNOWFLAKE_PASSWORD=xxxx
SNOWFLAKE_DATABASE=STOCK_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS="xxxx"
KAFKA_TOPIC_BATCH="xxxx"
KAFKA_GROUP_BATCH_ID="xxxx"
KAFKA_TOPIC_REALTIME="xxxx"
KAFKA_GROUP_BATCH_ID="xxxx"
KAFKA_GROUP_REALTIME_ID="xxxx"

# Minio config
MINIO_ACCESS_KEY="xxxx"
MINIO_SECRET_KEY="xxxx"
MINIO_BUCKET="stock-market-data"
```
---

##🚀 4. Start All Services

```bash
docker-compose up -d
```
This command starts the following services:

1.Apache Kafka + Zookeeper
2.Apache Spark (Master + Worker)
3.Apache Airflow (Webserver + Scheduler)
4.MinIO
5.PostgreSQL
---
## 🌐 5. Access Web Interfaces

| Service     | URL                    |
|------------|------------------------|
| Airflow UI | http://localhost:8080 |
| MinIO UI   | http://localhost:9001 |
| Spark UI   | http://localhost:8081 |
| Kafka UI   | http://localhost:9021 |

---
## 🔄 Workflow

### 🔹 Batch Producer
- Fetches 5 years of historical stock market data from Yahoo Finance  
- Sends data to Kafka topic:  
  `stock-market-batch`

---

### 🔹 Real-Time Producer
- Simulates real-time stock price updates  
- Sends data to Kafka topic:  
  `stock-market-realtime`

---

### 🔹 Spark Batch Processor
- Reads historical data from Kafka / MinIO  
- Calculates:
  - OHLC (Open, High, Low, Close)
  - Trading volumes  
- Stores processed data in MinIO using **Parquet format**

---

### 🔹 Spark Streaming Processor
- Consumes real-time data from Kafka  
- Computes:
  - Moving averages  
  - Volatility metrics  
- Stores output in MinIO

---

### 🔹 Airflow DAG
Orchestrates:
- Data ingestion  
- Spark batch and streaming jobs  
- Snowflake data loading  
- Data validation and monitoring  

<img width="2499" height="923" alt="Screenshot 2025-11-09 221034" src="https://github.com/user-attachments/assets/3f8b4bc5-57b6-4a35-8843-8947c5edbab4" />

---

### 🔹 Snowflake
- Acts as the **final curated analytical layer**  
- Used by Business Intelligence tools for reporting and analytics  

<img width="2401" height="1282" alt="Screenshot 2025-11-09 223102" src="https://github.com/user-attachments/assets/f7a99058-e28b-4fa5-8050-72b6e364bcae" />

---

### 🔹 Visualization
Power BI & Apache Superset dashboards for:

- Price trends  
- Volume distribution  
- Performance comparison  
- Real-time metrics  

<img width="2507" height="1411" alt="Screenshot 2025-11-09 190742" src="https://github.com/user-attachments/assets/a5ceee41-5d7e-4dd7-ae00-58d8960123fb" />

---

## 📊 Sample Dashboards

- Daily Performance (OHLC + Volume)  
- Trading Volume Distribution  
- Stock Price Trends  
- Real-Time Market Monitoring  
- Historical Comparison Across Stocks  

---
## 📌 Future Scope

- Machine Learning models for stock price prediction (LSTM, XGBoost)  
- Sentiment analysis using news & social media data  
- Kubernetes deployment for large-scale production environments  
- Real-time alerting system for trading signals and anomalies  
- REST APIs for external consumers  
- Algorithmic trading strategy backtesting 
