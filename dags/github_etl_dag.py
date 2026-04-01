import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from github_etl import main

with DAG(
    dag_id="github_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=main,
    )
