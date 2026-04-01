from datetime import datetime
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = "/Users/homesachin/Desktop/zoneone/practice/pharma-data-platform-mvp"


def extract_csv():
    # Extraction is handled inside run_pipeline.py
    print("extract_csv step initialized")


def clean_transform():
    # Transform is handled inside run_pipeline.py
    print("clean_transform step initialized")


def load_to_snowflake_or_local():
    env = os.environ.copy()
    subprocess.run(["python", f"{BASE_DIR}/etl/run_pipeline.py"], check=True, env=env)


with DAG(
    dag_id="pharma_data_platform_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pharma", "snowflake", "mdm"],
) as dag:
    t1 = PythonOperator(task_id="extract_csv", python_callable=extract_csv)
    t2 = PythonOperator(task_id="clean_transform", python_callable=clean_transform)
    t3 = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_snowflake_or_local)

    t1 >> t2 >> t3
