from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

PROJECT_ID = "rajesh-ecp-project"
REGION = "asia-southeast1"
CLUSTER_NAME = "lloyds-bank"

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://rajesh-demo-project/pyspark/customer_job.py"
    },
}

with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    # Start Task
    start = EmptyOperator(
        task_id="start"
    )

    # Dataproc PySpark Task
    run_pyspark = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # End Task
    end = EmptyOperator(
        task_id="end"
    )

    # Task Flow
    start >> run_pyspark >> end