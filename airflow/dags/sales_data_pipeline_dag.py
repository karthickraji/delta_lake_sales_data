from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from config.basic_config import HDFS_BRONZE_PATH, HDFS_SILVER_PATH

PROJECT_PATH = "/home/karthick/PycharmProjects/delta_lake_sales_data"

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def has_bronze_data():
    import os
    if not os.path.exists(HDFS_BRONZE_PATH):
        raise ValueError("The data is not found in Bronze folder!")

def has_silver_data():
    import os
    if not os.path.exists(HDFS_SILVER_PATH):
        raise ValueError("The data is not found in Silver folder!")

with DAG(
    dag_id="delta_lake_sales_data_pipeline",
    start_date=datetime(2026, 3, 12),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    default_args=default_args
) as dag:

    extract = SparkSubmitOperator(
        task_id="extract_data",
        application=f"{PROJECT_PATH}/spark_jobs/ingest.py",
        py_files=f"{PROJECT_PATH}/spark_jobs/dependencies.zip",
        conn_id="spark_default"
    )

    bronze_data_check = PythonOperator(
        task_id="bronze_data_check",
        python_callable=has_bronze_data
    )

    transform = SparkSubmitOperator(
        task_id="transform_data",
        application=f"{PROJECT_PATH}/spark_jobs/clean_and_transform.py",
        py_files=f"{PROJECT_PATH}/spark_jobs/dependencies.zip",
        conn_id="spark_default"
    )

    silver_data_check = PythonOperator(
        task_id="silver_data_check",
        python_callable=has_silver_data
    )

    load_aggregate_data_to_gold = SparkSubmitOperator(
        task_id="aggregate_data",
        application=f"{PROJECT_PATH}/spark_jobs/load.py",
        py_files=f"{PROJECT_PATH}/spark_jobs/dependencies.zip",
        conn_id="spark_default"
    )

    extract >> bronze_data_check >> transform >> silver_data_check >> load_aggregate_data_to_gold