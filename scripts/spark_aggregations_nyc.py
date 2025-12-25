#!/usr/bin/env python3
# twitter_project/scripts/spark_aggregations_nyc.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg, count, to_timestamp, date_format, hour
import os
from pathlib import Path

# Project paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "nyc_taxi"
AGG_DIR = PROJECT_ROOT / "data" / "aggregated_output" / "nyc_taxi"
AGG_DIR.mkdir(parents=True, exist_ok=True)

# Create Spark session and tune shuffle partitions based on CPU
cpu = os.cpu_count() or 4
spark = SparkSession.builder \
    .appName("NYCTaxiAggregations") \
    .config("spark.sql.shuffle.partitions", str(max(8, cpu * 2))) \
    .getOrCreate()

# Read all processed parquet files
input_pattern = str(PROCESSED_DIR / "*.parquet")
print("[spark] reading processed parquet from:", input_pattern)
df = spark.read.parquet(input_pattern)

# normalize column names to lower-case (spark already preserves them from parquet)
cols = [c.lower() for c in df.columns]
# If pickup datetime exists, parse it
pickup_col_candidates = [c for c in df.columns if "pickup" in c and "datetime" in c]
pickup_col = pickup_col_candidates[0] if pickup_col_candidates else None
if pickup_col:
    # ensure timestamp type and derive date/hour
    df = df.withColumn("pickup_ts", to_timestamp(col(pickup_col)))
    df = df.withColumn("trip_date", date_format(col("pickup_ts"), "yyyy-MM-dd"))
    df = df.withColumn("trip_hour", hour(col("pickup_ts")))
else:
    print("[spark] pickup timestamp column not found; date/hour aggregations will be skipped.")

# Ensure numeric columns exist
for numeric in ["trip_distance", "fare_amount", "passenger_count"]:
    if numeric not in df.columns:
        print(f"[spark] warning: expected numeric column '{numeric}' not present in dataframe columns {df.columns}")

# Trip distance categories
df = df.withColumn("trip_category",
                   when(col("trip_distance") < 2, "Short")
                   .when((col("trip_distance") >= 2) & (col("trip_distance") <= 8), "Medium")
                   .otherwise("Long"))

# Fare categories
df = df.withColumn("fare_category",
                   when(col("fare_amount") < 10, "Low")
                   .when((col("fare_amount") >= 10) & (col("fare_amount") <= 50), "Medium")
                   .otherwise("High"))

# --- Aggregation: passenger_count + trip_category (averages) ---
agg_by_trip = df.groupBy("passenger_count", "trip_category").agg(
    avg("trip_distance").alias("avg_trip_distance"),
    avg("fare_amount").alias("avg_fare_amount"),
    count("*").alias("count")
)

# --- Aggregation: fare_category counts & averages ---
agg_by_fare = df.groupBy("fare_category").agg(
    count("*").alias("count"),
    avg("fare_amount").alias("avg_fare")
)

# --- Aggregation: time-series by date & hour (if available) ---
if "trip_date" in df.columns and "trip_hour" in df.columns:
    agg_by_time = df.groupBy("trip_date", "trip_hour").agg(
        count("*").alias("trips"),
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_trip_distance")
    )
else:
    agg_by_time = None

# Write parquet outputs (efficient for big data)
print("[spark] writing parquet outputs to:", AGG_DIR)
agg_by_trip.write.mode("overwrite").parquet(str(AGG_DIR / "agg_by_trip_parquet"))
agg_by_fare.write.mode("overwrite").parquet(str(AGG_DIR / "agg_by_fare_parquet"))
if agg_by_time is not None:
    agg_by_time.write.mode("overwrite").parquet(str(AGG_DIR / "agg_by_time_parquet"))

# Also write small CSV summaries (coalesced -> single file) for quick viz tools
print("[spark] writing CSV summaries (single file) to:", AGG_DIR)
agg_by_trip.coalesce(1).write.mode("overwrite").option("header", True).csv(str(AGG_DIR / "agg_by_trip_csv"))
agg_by_fare.coalesce(1).write.mode("overwrite").option("header", True).csv(str(AGG_DIR / "agg_by_fare_csv"))
if agg_by_time is not None:
    agg_by_time.coalesce(1).write.mode("overwrite").option("header", True).csv(str(AGG_DIR / "agg_by_time_csv"))

print("[spark] aggregations completed.")
spark.stop()
