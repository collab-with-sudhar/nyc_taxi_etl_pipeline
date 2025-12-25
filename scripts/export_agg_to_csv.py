#!/usr/bin/env python3
# twitter_project/scripts/export_agg_to_csv.py
import os
import glob
import shutil
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
AGG_DIR = PROJECT_ROOT / "data" / "aggregated_output" / "nyc_taxi"
EXPORT_DIR = PROJECT_ROOT / "data" / "processed" / "csv_exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# mapping of source csv-folder -> output filename
sources = {
    "agg_by_trip_csv": "nyc_agg_by_trip.csv",
    "agg_by_fare_csv": "nyc_agg_by_fare.csv",
    "agg_by_time_csv": "nyc_agg_by_time.csv"
}

def flatten_and_move(src_folder_name, out_filename):
    src_folder = AGG_DIR / src_folder_name
    if not src_folder.exists():
        print(f"[export] source folder not found: {src_folder} (skipping)")
        return False
    parts = list(src_folder.glob("part-*.csv"))
    if not parts:
        print(f"[export] no part-*.csv found in {src_folder} (skipping)")
        return False
    out_path = EXPORT_DIR / out_filename
    # remove existing out_path
    if out_path.exists():
        out_path.unlink()
    shutil.move(str(parts[0]), str(out_path))
    # remove Spark _SUCCESS and any remaining files in folder
    for f in src_folder.iterdir():
        try:
            if f.is_file():
                f.unlink()
        except Exception:
            pass
    # keep folder but clean
    print(f"[export] exported {out_path}")
    return True

def main():
    for src, fname in sources.items():
        flatten_and_move(src, fname)
    print("[export] done. CSVs available at:", EXPORT_DIR)

if __name__ == "__main__":
    main()
