from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from scripts.transform_sales import transform_sales_data
import requests

def start():
    print("Starting Sales ETL Pipeline")

def call_external_api():
    response = requests.get("https://api.example.com/sales/latest")
    print(f"API response: {response.status_code}")

with DAG(
    dag_id="etl_sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sales", "etl"]
) as dag:

    start_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=start
    )

    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_sales_file",
        bucket_name="sales-data-bucket",
        bucket_key="daily/sales_{{ ds }}.csv",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=600
    )

    transform_task = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales_data
    )

    call_api_task = PythonOperator(
        task_id="call_external_api",
        python_callable=call_external_api
    )

    trigger_other_dag = TriggerDagRunOperator(
        task_id="trigger_reporting_pipeline",
        trigger_dag_id="etl_reporting_pipeline",
        wait_for_completion=True
    )

    # Task dependencies
    start_task >> wait_for_s3_file >> transform_task
    transform_task >> call_api_task
    transform_task >> trigger_other_dag
