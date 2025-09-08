import os
import re
import json
import time
import logging
from datetime import datetime, date

import pandas as pd
import requests
from google.cloud import storage

# Setting Paths and constants
CLICKSTREAM_PATH = "clickstream.csv"
TRANSACTIONS_PATH = "transactions.csv"
BUCKET_NAME = "storypoints-ai-data-bucket"
API_KEY = "b74bf2ded174e3515ea87712"  
API_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"

CHUNK_SIZE = 50_000
LOCAL_PROCESSED_DIR = "data/processed"
RAW_API_DIR = "data/raw/api_currency"
INGEST_DATE = date.today().strftime("%Y-%m-%d")

# Set Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# create directory
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

# Fetch USD-based conversion rates via API and save raw JSON (Task 2 + Task 5)
def fetch_exchange_rates() -> dict:  
    try:
        resp = requests.get(API_URL, timeout=20)
        data = resp.json()
    except Exception as e:
        logging.error(f"API request error: {e}")  # Task 5
        raise

    if resp.status_code == 200 and data.get("result") == "success":
        # Archive raw JSON by date
        out_dir = os.path.join(RAW_API_DIR, INGEST_DATE)
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, "rates.json")
        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved raw rates JSON → {out_path}")  # Task 5
        return data["conversion_rates"]

    logging.error(f"API failed: {data}")  # Task 5
    raise RuntimeError(f"ExchangeRate API failed: {data}")

# Standardize DataFrame column names to snake_case (Task 3)
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:  
    df.columns = [re.sub(r"__+", "_", re.sub(r"[^\w]+", "_", str(c).strip())).lower() for c in df.columns]  
    return df

# Upload a local file to Google Cloud Storage (Task 4)
def upload_to_gcs(local_file: str, gcs_path: str) -> None: 
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file)
    logging.info(f"☁️ Uploaded {local_file} → gs://{BUCKET_NAME}/{gcs_path}")

# ETL Functions

# Extract, clean, deduplicate, and load clickstream dataset (Tasks 2–4)
def process_clickstream() -> None:
    if not os.path.exists(CLICKSTREAM_PATH):
        logging.warning(f"Missing input: {CLICKSTREAM_PATH}")
        return

    ensure_dir(LOCAL_PROCESSED_DIR)
    records_in, records_out = 0, 0
    chunks = []

    for chunk in pd.read_csv(CLICKSTREAM_PATH, chunksize=CHUNK_SIZE): # Read CSV in chunks
        records_in += len(chunk)
        chunk = standardize_columns(chunk)

        if "click_time" in chunk.columns:
            chunk["click_time"] = pd.to_datetime(chunk["click_time"], utc=True, errors="coerce") # Parse click_time to UTC datetime

        chunks.append(chunk)

    if not chunks:
        logging.warning("No clickstream chunks read.")
        return

    # Combine chunks and deduplicate
    df = pd.concat(chunks, ignore_index=True)
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    records_out = after
    deduped = before - after

    # Save locally
    local_out = os.path.join(LOCAL_PROCESSED_DIR, f"clickstream_clean_{INGEST_DATE}.csv")
    df.to_csv(local_out, index=False)
    logging.info(
        f"Clickstream → in:{records_in} out:{records_out} deduped:{deduped} saved:{local_out}"
    )

    # Upload to GCS partitioned by ingest_date
    gcs_path = f"processed/clickstream/ingest_date={INGEST_DATE}/clickstream.csv"
    upload_to_gcs(local_out, gcs_path)


# Extract, clean, enrich, deduplicate, and load transactions dataset (Tasks 2–4)
def process_transactions(rates: dict) -> None:
    if not os.path.exists(TRANSACTIONS_PATH):
        logging.warning(f"Missing input: {TRANSACTIONS_PATH}")
        return

    ensure_dir(LOCAL_PROCESSED_DIR)

    df = pd.read_csv(TRANSACTIONS_PATH)
    records_in = len(df)
    df = standardize_columns(df)

    # Convert txn_time to UTC
    if "txn_time" in df.columns:
        df["txn_time"] = pd.to_datetime(df["txn_time"], utc=True, errors="coerce")

    # Enrich with amount_in_usd using exchange rates
    if {"amount", "currency"}.issubset(df.columns):
        def to_usd(row):
            cur = str(row["currency"]).upper()
            amt = row["amount"]
            rate = rates.get(cur)
            if rate and rate != 0:
                return amt / rate
            return pd.NA

        df["amount_in_usd"] = df.apply(to_usd, axis=1)

        # Warn on currencies we couldn’t convert
        missing_cur = sorted(c for c in df["currency"].str.upper().unique() if c not in rates)
        if missing_cur:
            logging.warning(f"No rates for currencies: {missing_cur}")
    else:
        logging.warning("Expected columns 'amount' and 'currency' not found; skipping USD enrichment.")

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    deduped = before - after

    # Save locally
    local_out = os.path.join(LOCAL_PROCESSED_DIR, f"transactions_clean_{INGEST_DATE}.csv")
    df.to_csv(local_out, index=False)
    logging.info(
        f"Transactions → in:{records_in} out:{after} deduped:{deduped} saved:{local_out}"
    )

    gcs_path = f"processed/transactions/ingest_date={INGEST_DATE}/transactions.csv"
    upload_to_gcs(local_out, gcs_path)

# Main - Run the full ETL pipeline (Tasks 2–5)
def main():
    logging.info("Starting ETL pipeline (Week 1)")
    rates = fetch_exchange_rates()
    logging.info("Exchange rates fetched")

    process_clickstream()
    process_transactions(rates)

    logging.info("ETL pipeline finished")

if __name__ == "__main__":
    main()
