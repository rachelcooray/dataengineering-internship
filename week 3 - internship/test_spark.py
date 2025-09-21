from pyspark.sql import SparkSession

def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Week3-TestJob") \
        .getOrCreate()

    # Read clickstream CSV (from Week 1 data)
    df = spark.read.csv("gs://storypoints-ai-data-bucket/week3/clickstream.csv", header=True, inferSchema=True)

    # Print schema
    df.printSchema()

    # Show sample rows
    df.show(5, truncate=False)

    # Simple aggregation: count rows
    print("Total rows:", df.count())

    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    main()
