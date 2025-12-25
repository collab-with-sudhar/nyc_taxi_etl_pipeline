import pandas as pd
import matplotlib.pyplot as plt
import os

# Path to CSV exports (from ETL pipeline)
csv_dir = "/root/airflow_session/twitter_project/data/processed/csv_exports"

# ✅ Windows directory to save visualizations
windows_output_dir = "/mnt/c/Users/<YourWindowsUser>/Documents/tableau_data/visualizations"

# Create directory if it doesn't exist
os.makedirs(windows_output_dir, exist_ok=True)

# Find the most recent CSV export
files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]
if not files:
    raise FileNotFoundError("No CSV files found in csv_exports/")

latest_file = max([os.path.join(csv_dir, f) for f in files], key=os.path.getctime)

print(f"📂 Reading data from: {latest_file}")
df = pd.read_csv(latest_file)

# Check for expected columns
expected_cols = {"trip_category", "avg_fare_amount", "avg_trip_distance", "total_revenue"}
missing = expected_cols - set(df.columns)
if missing:
    print(f"⚠️ Warning: Missing columns in dataset: {missing}")

# 1️⃣ Plot: Average Fare by Trip Category
if "trip_category" in df.columns and "avg_fare_amount" in df.columns:
    plt.figure(figsize=(8, 5))
    df.plot.bar(x="trip_category", y="avg_fare_amount", legend=False)
    plt.title("Average Fare by Trip Category")
    plt.ylabel("Average Fare ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(windows_output_dir, "avg_fare_by_category.png"))
    plt.close()
    print("✅ Saved avg_fare_by_category.png")

# 2️⃣ Plot: Average Trip Distance by Category
if "trip_category" in df.columns and "avg_trip_distance" in df.columns:
    plt.figure(figsize=(8, 5))
    df.plot.bar(x="trip_category", y="avg_trip_distance", legend=False)
    plt.title("Average Trip Distance by Category")
    plt.ylabel("Average Distance (miles)")
    plt.tight_layout()
    plt.savefig(os.path.join(windows_output_dir, "avg_distance_by_category.png"))
    plt.close()
    print("✅ Saved avg_distance_by_category.png")

# 3️⃣ Plot: Total Revenue by Category
if "trip_category" in df.columns and "total_revenue" in df.columns:
    plt.figure(figsize=(8, 5))
    df.plot.bar(x="trip_category", y="total_revenue", legend=False)
    plt.title("Total Revenue by Trip Category")
    plt.ylabel("Total Revenue ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(windows_output_dir, "total_revenue_by_category.png"))
    plt.close()
    print("✅ Saved total_revenue_by_category.png")

print(f"🎉 All charts saved successfully to: {windows_output_dir}")
