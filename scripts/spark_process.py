from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, length
import argparse
import os

def main(input_path, output_path, master):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Twitter Sentiment Processing") \
        .master(master) \
        .getOrCreate()

    # Load CSV file
    df = spark.read.option("header", True).csv(input_path)

    # Show schema to confirm columns
    print("Schema detected:")
    df.printSchema()

    # Clean the tweet text
    df = df.withColumn("tweet_clean", regexp_replace(col("tweet"), "(https?://\\S+)", ""))
    df = df.withColumn("tweet_clean", regexp_replace(col("tweet_clean"), "[^a-zA-Z\\s]", ""))
    df = df.withColumn("tweet_clean", regexp_replace(col("tweet_clean"), "\\s+", " "))

    # Add a simple tweet length metric
    df = df.withColumn("tweet_length", length(col("tweet_clean")))

    # Group by sentiment label and compute average tweet length
    df_summary = df.groupBy("label").agg({"tweet_length": "avg"}).withColumnRenamed("avg(tweet_length)", "avg_tweet_length")

    # Save as Parquet
    df_summary.write.mode("overwrite").parquet(output_path)

    print("✅ Data processing completed successfully!")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Twitter Sentiment Dataset")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output directory for Parquet")
    parser.add_argument("--master", default="local[2]", help="Spark master (default: local[2])")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    main(args.input, args.output, args.master)
