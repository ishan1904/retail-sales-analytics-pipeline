from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# 1️⃣ DAG metadata + retries
default_args = {
    "owner": "ishan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 2️⃣ Define the DAG
with DAG(
    dag_id="retail_etl_pipeline",
    default_args=default_args,
    description="ETL for Retail Sales Analytics - runs etl_pipeline.py",
    start_date=datetime(2024, 9, 1),
    schedule_interval="@daily",  # change to None if you only want to run manually
    catchup=False
) as dag:

    # 3️⃣ Task: Run the ETL script
    def run_etl_script():
        # Path inside the Docker container
        script_path = "/opt/airflow/src/etl_pipeline.py"
        result = subprocess.run(["python", script_path], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"ETL failed:\n{result.stderr}")

    run_etl = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl_script
    )

    run_etl
