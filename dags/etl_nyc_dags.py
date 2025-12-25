from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 1),
}

PROJECT_HOME = "/root/airflow_session/twitter_project"
VENV_ACTIVATE = "source /root/airflow_session/airflow_env/bin/activate"

with DAG(
    "nyc_taxi_etl",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id="data_ingestion",
        bash_command=f"{VENV_ACTIVATE} && python {PROJECT_HOME}/scripts/data_ingestion_nyc.py",
    )

    spark_agg = BashOperator(
        task_id="spark_aggregations",
        bash_command=f"{VENV_ACTIVATE} && spark-submit --master local[*] {PROJECT_HOME}/scripts/spark_aggregations_nyc.py",
    )

    export_csv = BashOperator(
        task_id="export_agg_to_csv",
        bash_command=f"{VENV_ACTIVATE} && python {PROJECT_HOME}/scripts/export_agg_to_csv.py",
    )

    copy_to_windows = BashOperator(
   	 task_id='copy_to_windows',
    	bash_command="""
    	mkdir -p /mnt/c/Users/sudharshan/Documents/tableau_data &&
    	cp /root/airflow_session/twitter_project/data/processed/csv_exports/*.csv /mnt/c/Users/sudharshan/Documents/tableau_data/
    """
    )
