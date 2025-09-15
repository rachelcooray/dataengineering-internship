"""
log_utils.py
-------------
Utility functions for logging ETL metadata and alerts.
Used by Airflow DAGs in Week 2 orchestration.

Functions:
    log_metadata(dataset_name, rows_in, rows_out, validation_status, gcs_bucket)
    log_alert(message, gcs_bucket)
"""

import csv
import os
import logging
from datetime import datetime
from google.cloud import storage

# Local file paths
METADATA_FILE = os.path.join("orchestration", "metadata", "run_log.csv")
ALERTS_FILE = os.path.join("orchestration", "metadata", "alerts.log")

def log_metadata(dataset_name: str, rows_in: int, rows_out: int,
                 validation_status: str, gcs_bucket: str) -> None:
    """
    Append ETL run metadata to run_log.csv and upload to GCS.

    Args:
        dataset_name (str): Name of dataset (e.g., 'clickstream')
        rows_in (int): Number of rows read
        rows_out (int): Number of rows after processing
        validation_status (str): 'PASS' or 'FAIL'
        gcs_bucket (str): Target GCS bucket name
    """
    ingest_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = [dataset_name, rows_in, rows_out, validation_status, ingest_time]

    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    file_exists = os.path.isfile(METADATA_FILE)

    with open(METADATA_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["dataset", "rows_in", "rows_out", "validation_status", "timestamp"])
        writer.writerow(row)

    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob("metadata/run_log.csv")
    blob.upload_from_filename(METADATA_FILE)
    logging.info(f"Metadata logged for {dataset_name} → {METADATA_FILE} and uploaded to GCS.")


def log_alert(message: str, gcs_bucket: str) -> None:
    """
    Append alert messages to alerts.log and upload to GCS.

    Args:
        message (str): Alert/error message
        gcs_bucket (str): Target GCS bucket name
    """
    alert_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"{alert_time} | {message}"

    with open(ALERTS_FILE, "a") as f:
        f.write(formatted_message + "\n")

    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob("alerts/alerts.log")
    blob.upload_from_filename(ALERTS_FILE)
    logging.error(f"ALERT: {formatted_me_
