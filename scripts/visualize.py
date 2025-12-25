import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os

def main(input_path, output_path):
    # Read Parquet into pandas
    df = pd.read_parquet(input_path)
    print("Loaded data:")
    print(df)

    # Plot average tweet length by sentiment
    plt.figure(figsize=(6,4))
    plt.bar(df['label'], df['avg_tweet_length'], color=['red', 'green'])
    plt.xticks([0,1], ['Negative', 'Positive'])
    plt.title("Average Tweet Length by Sentiment")
    plt.xlabel("Sentiment")
    plt.ylabel("Average Tweet Length")
    plt.tight_layout()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path)
    print(f"✅ Visualization saved to {output_path}")

if __name__ == "__main__":
    input_path = "/root/airflow_session/twitter_project/output/processed/"
    output_path = "/root/airflow_session/twitter_project/viz/out/sentiment_by_length.png"
    main(input_path, output_path)
