from pyspark.sql import SparkSession
from pyspark.sql.functions import count, length, avg

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Twitter Spark Aggregations") \
    .getOrCreate()

# File paths (update if needed)
input_path = "/root/airflow_session/twitter_project/data/sentiment140.csv"
output_path = "/root/airflow_session/twitter_project/data/aggregated_output"

# Read CSV into DataFrame
df = spark.read.option("header", True).csv(input_path)

# Ensure the columns exist
print("Columns in CSV:", df.columns)

# Aggregation 1: Count of tweets per label
agg_label_count = df.groupBy("label").agg(
    count("*").alias("tweet_count")
)

# Aggregation 2: Average tweet length per label
df = df.withColumn("tweet_length", length(df["tweet"]))
agg_avg_length = df.groupBy("label").agg(
    avg("tweet_length").alias("avg_tweet_length")
)

# Show results in console
print("Tweet count per label:")
agg_label_count.show()

print("Average tweet length per label:")
agg_avg_length.show()

# Write aggregated results to CSV (separate folders)
agg_label_count.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/label_count")
agg_avg_length.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/avg_length")

# Stop Spark session
spark.stop()
