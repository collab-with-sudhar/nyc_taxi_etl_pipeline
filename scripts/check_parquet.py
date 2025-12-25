# scripts/check_parquet.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CheckParquet").getOrCreate()
p = "twitter_project/output/processed"   # <- change if your parquet location differs
df = spark.read.parquet(p)

print("=== SCHEMA ===")
df.printSchema()

print("=== SAMPLE ROWS ===")
df.show(10, truncate=False)

print("=== ROW COUNT ===")
print(df.count())

# If there is a label column, show counts by label
for possible_label in ("label", "polarity", "sentiment"):
    if possible_label in df.columns:
        print(f"=== value counts for column '{possible_label}' ===")
        df.groupBy(possible_label).count().orderBy("count", ascending=False).show()
        break

spark.stop()
