from datasets import load_dataset
import os
from pyspark.sql import SparkSession

# ✅ Create Spark session
spark = SparkSession.builder.appName("AmazonReviewsIngestion").getOrCreate()

# ✅ Download a subset (~120 MB) of Amazon 2023 reviews dataset
print("Downloading dataset...")
dataset = load_dataset("McAuley-Lab/Amazon-Reviews-2023", "Electronics", split="train[:1%]")  
# use [:1%] to keep it around 100–150MB; increase to [:5%] if you want ~500MB

# ✅ Save locally
raw_path = "twitter_project/data/raw/amazon_reviews"
os.makedirs(raw_path, exist_ok=True)

out_json = os.path.join(raw_path, "amazon_electronics_sample.jsonl")
dataset.to_json(out_json, orient="records", lines=True)
print(f"Saved sample to {out_json}")

# ✅ Read with Spark and save as Parquet (for faster ETL)
df = spark.read.json(out_json)
df.write.mode("overwrite").parquet("twitter_project/data/processed/amazon_reviews_parquet")

print("✅ Data saved as parquet.")
spark.stop()
