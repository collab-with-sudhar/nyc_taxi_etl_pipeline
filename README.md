# NYC Taxi ETL Pipeline
This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow and Apache Spark to process NYC Yellow Taxi trip data. The pipeline ingests Parquet datasets, performs large-scale transformations and aggregations using PySpark, exports cleaned CSV files, and optionally generates visualizations for BI tools such as Tableau.

## Tech Stack
- Python 3.12  
- Apache Airflow  
- Apache Spark (PySpark)  
- Pandas, PyArrow, NumPy  
- Matplotlib / Plotly  
- WSL2 (Ubuntu on Windows)

## Dataset
Source: NYC TLC Trip Record Data (Yellow Taxi)  
Format: Parquet  
Example file: yellow_tripdata_2023-01.parquet  
Dataset link: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
Note: Dataset files are not included in this repository.

## Setup
Prerequisites: WSL2 (Ubuntu), Python 3.12, Java 11, Apache Spark

Create and activate virtual environment:
```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
pip install --upgrade pip
pip install apache-airflow==2.7.1 pandas==1.5.3 pyarrow==14.0.2 numpy<2 pyspark==3.4.1 requests matplotlib plotly kaleido
python nyc_taxi_project/scripts/data_ingestion_nyc.py
spark-submit nyc_taxi_project/scripts/spark_transformations_nyc.py
python nyc_taxi_project/scripts/export_to_csv.py
python nyc_taxi_project/scripts/generate_visualizations.py
airflow db init
airflow webserver -p 8080
airflow scheduler
Open http://localhost:8080
 and trigger the nyc_taxi_etl DAG.
