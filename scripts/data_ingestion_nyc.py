#!/usr/bin/env python3
# twitter_project/scripts/data_ingestion_nyc.py
import os
import requests
import shutil
from pathlib import Path

# ---------------- Config ----------------
MONTHS = ["2023-01"]  # add more months if needed
URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
# ----------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "nyc_taxi"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "nyc_taxi"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def download_parquet(month: str) -> Path:
    url = URL_TEMPLATE.format(month=month)
    out_path = RAW_DIR / f"yellow_tripdata_{month}.parquet"
    if out_path.exists():
        print(f"[download] already exists: {out_path}")
        return out_path
    print(f"[download] fetching {url} -> {out_path}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print(f"[download] saved {out_path}")
    return out_path

def stage_to_processed(raw_path: Path) -> Path:
    # Instead of loading into memory, just copy/move the parquet file into processed folder.
    dst = PROCESSED_DIR / raw_path.name
    print(f"[stage] moving {raw_path} -> {dst}")
    # prefer move to save disk, fallback to copy
    try:
        shutil.move(str(raw_path), str(dst))
    except Exception:
        shutil.copy2(str(raw_path), str(dst))
    print(f"[stage] staged to {dst}")
    return dst

def main():
    downloaded = []
    for month in MONTHS:
        try:
            p = download_parquet(month)
            downloaded.append(p)
        except Exception as e:
            print(f"[error] download failed for {month}: {e}")
            raise

    processed = []
    for p in downloaded:
        try:
            processed_path = stage_to_processed(p)
            processed.append(processed_path)
        except Exception as e:
            print(f"[error] staging failed for {p}: {e}")
            raise

    print("[done] ingestion complete")
    print("processed files:", processed)

if __name__ == "__main__":
    main()
