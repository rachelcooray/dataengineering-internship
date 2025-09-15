"""
ETL Week 2 DAG - StoryPoints AI
Orchestrates:
1. Fetch currency API
2. Ingest clickstream
3. Ingest + validate transactions
4. Load to GCS (only if validation passes)
5. Log metadata & alerts
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Import Week 1 ETL functions

from etl_pipeline import process_clickstream, process_transactions, fetch_exchange_rates
from validation import validate_transactions
from log_utils import log_metadata, log_alert

# Config
BUCKET_NAME = os.environ.get("GCS_BUCKET", "us-central1-storypoints-ai--aa8817f2-bucket")

# Alert handler
def task_failure_alert(context):
    """Log alerts when any task fails"""
    task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    error = context.get('exception')
    log_alert(f"DAG={dag_id}, Task={task_id} failed: {error}", BUCKET_NAME)

# Default DAG args
default_args = {
    "owner": "rachelcooray",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.environ.get("ALERT_EMAIL", "rachelcooray@gmail.com")],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}

import pandas as pd

# Wrapper function for validation + metadata logging
def validate_and_process_transactions():
    rates = fetch_exchange_rates()
    df = process_transactions(rates)

    if df is None:
        logging.warning("process_transactions returned None, skipping validation.")
        log_metadata("transactions", 0, 0, "FAIL", BUCKET_NAME)
        return None

    # Run validation
    df_validated = validate_transactions(df)

    rows_in = len(df)
    rows_out = len(df_validated)
    status = "PASS" if rows_out == rows_in else "FAIL"

    # Log metadata
    log_metadata("transactions", rows_in, rows_out, status, BUCKET_NAME)

    # Fail DAG if validation failed
    if status == "FAIL":
        raise ValueError("Validation failed for transactions dataset")

    return df_validated

# Define DAG
with DAG(
    dag_id="etl_week2_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 13),
    schedule_interval="@daily",
    catchup=False,
    tags=["week2", "storypoints"],
) as dag:

    # Task 1: Fetch currency API
    fetch_currency_task = PythonOperator(
        task_id="fetch_currency_api",
        python_callable=fetch_exchange_rates,
    )

    # Task 2: Process clickstream
    process_clickstream_task = PythonOperator(
        task_id="process_clickstream",
        python_callable=process_clickstream,
    )

    # Task 3: Process + validate transactions
    process_transactions_task = PythonOperator(
        task_id="process_transactions",
        python_callable=validate_and_process_transactions,
    )

    # Task 4: Final load marker (runs only if all pass)
    finalize_task = PythonOperator(
        task_id="finalize_pipeline",
        python_callable=lambda: print("ETL pipeline completed successfully!"),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dependencies
    fetch_currency_task >> [process_clickstream_task, process_transactions_task]
    [process_clickstream_task, process_transactions_task] >> finalize_task
