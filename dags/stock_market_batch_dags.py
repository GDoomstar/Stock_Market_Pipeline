import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "stock_market_batch_pipeline",
    default_args=default_args,
    description="Stock Market Data Pipeline - 5 Years Data",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 4, 1),
    catchup=False,
)

# Calculate date range for 5 years
five_years_ago = "{{ (execution_date - macros.timedelta(days=5*365)).strftime('%Y-%m-%d') }}"
current_date = "{{ ds }}"

# Task to fetch 5 years of historical data
fetch_historical_data = BashOperator(
    task_id="fetch_historical_data",
    bash_command=f"python /opt/airflow/dags/scripts/batch_data_producer.py {five_years_ago} {current_date}",
    dag=dag,
)

# Task to consume historical data
consume_historical_data = BashOperator(
    task_id="consume_historical_data",
    bash_command=f"python /opt/airflow/dags/scripts/batch_data_consumer.py {five_years_ago} {current_date} 200 600",
    dag=dag,
)

# Spark processing task for 5 years data
process_data = BashOperator(
    task_id="process_data",
    bash_command=f"""
    docker exec stockmarketdatapipeline-spark-master-1 \
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
            /opt/spark/jobs/spark_batch_processor.py {five_years_ago} {current_date}
    """,
    dag=dag,
)

# Updated Snowflake loader for 5 years data
load_to_snowflake = BashOperator(
    task_id="load_historical_to_snowflake",
    bash_command=f"python /opt/airflow/dags/scripts/load_to_snowflake.py {five_years_ago} {current_date}",
    dag=dag
)

process_complete = BashOperator(
    task_id="process_complete",
    bash_command=f'echo "5 Years Batch Process from {five_years_ago} to {current_date} is complete"',
    dag=dag
)

fetch_historical_data >> consume_historical_data >> process_data >> load_to_snowflake >> process_complete