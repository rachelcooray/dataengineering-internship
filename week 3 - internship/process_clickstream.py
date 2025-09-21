from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
import gcsfs
from datetime import date

# Constants
CLICKSTREAM_PATH = "gs://storypoints-ai-data-bucket/week3/clickstream.csv"
OUTPUT_PATH = "gs://storypoints-ai-data-bucket/week3/output/clickstream_parquet/"
ERROR_PATH = "gs://storypoints-ai-data-bucket/week3/output/clickstream_errors/"
RUN_LOG_PATH = "gs://storypoints-ai-data-bucket/week3/metadata/run_log.csv"
INGEST_DATE = date.today().strftime("%Y-%m-%d")

def log_run(dataset: str, rows_in: int, rows_out: int, validation_status: str = "success"):
    fs = gcsfs.GCSFileSystem()
    timestamp = date.today().isoformat()
    row = f"{dataset},{rows_in},{rows_out},{validation_status},{timestamp}\n"
    
    if fs.exists(RUN_LOG_PATH):
        with fs.open(RUN_LOG_PATH, "ab") as f:
            f.write(row.encode("utf-8"))
    else:
        header = "dataset,rows_in,rows_out,validation_status,timestamp\n"
        with fs.open(RUN_LOG_PATH, "wb") as f:
            f.write(header.encode("utf-8"))
            f.write(row.encode("utf-8"))

def main():
    spark = SparkSession.builder.appName("Week3-ClickstreamProcessing").getOrCreate()

    # Read clickstream
    df = spark.read.csv(CLICKSTREAM_PATH, header=True, inferSchema=True)

    # Parse click_time
    df = df.withColumn("click_time", to_timestamp("click_time", "yyyy-MM-dd HH:mm:ss"))

    # Split valid/invalid rows
    valid_df = df.filter(
        col("user_id").isNotNull() & col("session_id").isNotNull() & col("click_time").isNotNull()
    )
    invalid_df = df.subtract(valid_df)

    # Deduplicate by session_id
    valid_df = valid_df.dropDuplicates(["session_id"])

    # Add partition column
    valid_df = valid_df.withColumn("click_date", to_date(col("click_time")))

    # Debug: show some rows before writing
    print("Sample valid rows:")
    valid_df.show(5, truncate=False)
    print("Total valid rows:", valid_df.count())

    # Write valid rows partitioned by click_date
    valid_df.write.mode("overwrite").partitionBy("click_date").parquet(OUTPUT_PATH)

    # Write invalid rows
    if invalid_df.count() > 0:
        invalid_df.write.mode("overwrite").parquet(ERROR_PATH)
        print(f"Invalid rows written to {ERROR_PATH}")

    # Log run
    log_run("clickstream", df.count(), valid_df.count(), "success")
    print("Clickstream - valid rows:", valid_df.count(), "invalid rows:", invalid_df.count())

    spark.stop()

if __name__ == "__main__":
    main()
