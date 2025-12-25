"""
download_helper.py
Downloads the Sentiment140 dataset directly from GitHub and saves it locally.
"""

import os
import requests
import pandas as pd

def main():
    print("📥 Downloading Sentiment140 dataset from GitHub...")

    # Direct GitHub CSV file URL
    url = "https://raw.githubusercontent.com/dD2405/Twitter_Sentiment_Analysis/master/train.csv"

    # Existing directory
    save_dir = "/root/airflow_session/twitter_project/data"
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, "sentiment140.csv")

    # Stream download for efficiency
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print(f"✅ Download complete! File saved at: {save_path}")

    # Optional: load sample for verification
    try:
        df = pd.read_csv(save_path)
        print(f"📊 Preview of dataset ({len(df)} rows):")
        print(df.head())
    except Exception as e:
        print(f"⚠️ Could not preview dataset due to: {e}")

if __name__ == "__main__":
    main()
