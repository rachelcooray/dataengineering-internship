from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, round
import requests

# Constants
API_KEY = "de1f36f23fe14f93642ba787"
API_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"

def fetch_rates():
    """Fetch exchange rates to USD from API."""
    resp = requests.get(API_URL, timeout=20)
    data = resp.json()
    if resp.status_code != 200 or data.get("result") != "success":
        raise RuntimeError(f"ExchangeRate API failed: {data}")
    return data["conversion_rates"]

def main():
    spark = SparkSession.builder.appName("Week3-TransactionsProcessing").getOrCreate()

    # Read transactions
    tx_df = spark.read.csv(
        "gs://storypoints-ai-data-bucket/week3/transactions.csv",
        header=True, inferSchema=True
    ).withColumn("txn_time", to_timestamp("txn_time", "yyyy-MM-dd HH:mm:ss"))

    # Fetch rates
    rates_dict = fetch_rates()
    # Ensure all rates are floats to avoid type conflicts
    rates_list = [(k, float(v)) for k, v in rates_dict.items()]
    rates_df = spark.createDataFrame(rates_list, ["currency", "rate_to_usd"])

    # Filter valid/invalid transactions
    valid_tx_df = tx_df.dropna(subset=["user_id", "txn_id", "txn_time", "amount", "currency"])
    invalid_tx_df = tx_df.subtract(valid_tx_df)

    # Enrich with USD
    enriched_df = valid_tx_df.join(rates_df, on="currency", how="left") \
        .withColumn("amount_in_usd", round(col("amount") * col("rate_to_usd"), 2))

    # Transactions with missing rates
    invalid_rate_df = enriched_df.filter(col("amount_in_usd").isNull())
    enriched_df = enriched_df.filter(col("amount_in_usd").isNotNull())

    # Partition column
    enriched_df = enriched_df.withColumn("txn_date", to_date(col("txn_time")))

    # Write valid transactions (partitioned by date, sorted by user_id)
    valid_output_path = "gs://storypoints-ai-data-bucket/week3/output/transactions_parquet/"
    enriched_df.repartition("txn_date") \
        .sortWithinPartitions("user_id") \
        .write.mode("overwrite") \
        .partitionBy("txn_date") \
        .parquet(valid_output_path)

    # Write invalid transactions (ensure same columns for union)
    invalid_rate_df_trimmed = invalid_rate_df.select("txn_id", "user_id", "amount", "currency", "txn_time")
    combined_invalid = invalid_tx_df.union(invalid_rate_df_trimmed)
    if combined_invalid.count() > 0:
        error_output_path = "gs://storypoints-ai-data-bucket/week3/output/transactions_errors/"
        combined_invalid.write.mode("overwrite").parquet(error_output_path)

    print("Transactions - valid rows:", enriched_df.count(), "invalid rows:", combined_invalid.count())
    spark.stop()

if __name__ == "__main__":
    main()
