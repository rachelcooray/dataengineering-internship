# StoryPoints AI – Data Engineering Internship (Week 1)

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline that extracts clickstream and transaction data, applies transformations, and stores cleaned outputs in Google Cloud Storage (GCS). It also fetches live currency exchange rates from the ExchangeRate API and enriches transaction data.

---

## Datasets
### clickstream.csv (200,000 rows)
- **Columns (after standardization)**: `user_id`, `session_id`, `page_url`, `click_time`  
- **Notes**:  
  - No null values in key identifiers  
  - `click_time` parsed to UTC  
  - No duplicates after cleaning  

### transactions.csv (100,000 rows)
- **Columns (after standardization)**: `transaction_id`, `user_id`, `amount`, `currency`, `txn_time`, `amount_in_usd`  
- **Notes**:  
  - Timestamps converted to UTC  
  - `amount_in_usd` derived using API conversion rates  
  - No duplicate rows after cleaning  

### ExchangeRate API
- Live rates fetched from `https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD`  
- Raw JSON stored daily at: `data/raw/api_currency/YYYY-MM-DD/rates.json`

![Screenshot of API raw JSON file saved locally](images/API_raw_JSON_file.png)

---

## Tasks Completed
- **Task 1**: Explored data, nulls, and duplicates. Documented above.  
- **Task 2**: Extracted CSVs in 50,000 chunks & fetched currency rates via API.  
- **Task 3**: Standardized columns, converted timestamps to UTC, deduplicated, enriched transactions with `amount_in_usd`.  
- **Task 4**: Loaded cleaned outputs into GCS, partitioned by `ingest_date=YYYY-MM-DD/`.  
- **Task 5**: Implemented logging for record counts, API errors, and missing inputs.  
- **Task 6**: Designed architecture diagram.  

---

## Output Structure
This shows how processed outputs are organized in **Google Cloud Storage (GCS)**:

![Screenshot of GCS bucket with partitioned outputs](images/GCS_bucket.png)

- Ensures each dataset is stored in a structured, partitioned way by ingestion date.  
- Makes it easier for analytics and downstream pipelines like BigQuery to query data by time period.    

---

## Logging & Alerts

The pipeline has structured logging and warning messages.  

**Used for**:  
- Records number of rows read, processed, and deduplicated.  
- Captures API request errors.  
- Prints **warnings** if:  
  - Input CSV files are missing  
  - Currency codes are not found in the exchange rates  
- Ensures visibility into ETL health without manual debugging.  

![Screenshot of log outputs from pipeline run](images/pipeline_logs.png)

---

## Architecture Diagram

![Architecture Diagram](images/architecture_diagram.png)

The architecture diagram shows how raw clickstream, transaction CSVs, and API data flow through the ETL pipeline (Extract -> Transform -> Load). Cleaned, partitioned outputs are stored in Google Cloud Storage, with logging and alerts ensuring pipeline reliability.

---

# StoryPoints AI – Data Engineering Internship (Week 2)

## Overview

In **Week 2**, the ETL pipeline from Week 1 was **operationalized using Cloud Composer (Airflow in GCP)**.
The goal was to orchestrate ingestion, apply **data validation before loading**, track metadata, and configure **alerts** for pipeline failures.

---

## Orchestration with Airflow

A new DAG (`etl_week2_dag.py`) was created to orchestrate the following tasks:

1. **Fetch currency API**

   * Pulls latest USD-based exchange rates and stores raw JSON.

2. **Process clickstream**

   * Extracts and cleans clickstream data (standardizes columns, parses timestamps, deduplicates).
   * Loads cleaned data into GCS partitioned by ingestion date.

3. **Process + validate transactions**

   * Cleans and enriches transaction data with `amount_in_usd`.
   * Applies **validation rules** before load (see below).
   * Logs metadata and stops pipeline if validation fails.

4. **Finalize pipeline**

   * Marks successful DAG completion only if all tasks succeed.

![Screenshot of DAG in Airflow UI](images/airflow_dag.png)

---

## Data Validation Rules

Validation was implemented in `/week2/orchestration/validation/validation.py`.
Checks applied to the **transactions dataset**:

* **No null values** in key fields (`transaction_id`, `amount`, `currency`).
* **Positive amounts only** (reject zero or negative).
* **Valid currency codes** (3-letter ISO format).
* **Consistency check**: number of rows in vs. out after cleaning.

If validation fails, the DAG:

* Logs a **FAIL status** in metadata.
* Raises an exception to stop downstream tasks.
* Triggers an **alert**.

---

## Metadata Tracking

Metadata is tracked in `/week2/orchestration/metadata/run_log.csv`, managed by `log_utils.py`.

Each pipeline run appends a new row:

![Screenshot of appended row](images/appended_row.png)

* **rows\_in**: records ingested.
* **rows\_out**: records after validation & deduplication.
* **validation\_status**: PASS or FAIL.
* **timestamp**: UTC ingestion timestamp.

---

## Monitoring & Alerts

Alerts are implemented in `log_utils.py` and triggered via Airflow `on_failure_callback`.

* **Alerts file**: `/week2/orchestration/metadata/alerts.log`
* On task failure:

  * Logs the error message with timestamp.
  * Uploads `alerts.log` to GCS (`alerts/alerts.log`).
  * Sends an **email alert** (configured via Airflow).

## Improvements over Week 1

* ✅ **Airflow DAG orchestration** (Composer).
* ✅ **Validation before load** (ensures clean, trusted data).
* ✅ **Metadata logging** to CSV + GCS.
* ✅ **Monitoring & alerts** via log files and Airflow email notifications.
* ✅ **Idempotence**: reruns do not duplicate data in GCS.

---