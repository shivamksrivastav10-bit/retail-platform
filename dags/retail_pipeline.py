from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Absolute Windows target path used for outer Docker socket binding redirection
HOST_DATA_DIR = "D:/projects/retail-platform/data"

default_args = {
    'owner': 'shivam',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'retail_medallion_pipeline',
    default_args=default_args,
    description='Automated Bronze, Silver, and Gold Medallion Spark Pipeline',
    schedule_interval=None,  # Manual trigger execution rule
    catchup=False,
) as dag:

    # Task 1: Execute the Bronze Ingestion Engine
    run_bronze = DockerOperator(
        task_id='spark_bronze_ingestion',
        image='retail-platform:latest',
        api_version='auto',
        auto_remove=True,
        command='sbt "runMain com.retail.bronze.BronzeJob"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[Mount(source=HOST_DATA_DIR, target="/app/data", type="bind")]
    )

    # Task 2: Execute the Silver Cleaning & Enrichment Engine
    run_silver = DockerOperator(
        task_id='spark_silver_transformation',
        image='retail-platform:latest',
        api_version='auto',
        auto_remove=True,
        command='sbt "runMain com.retail.silver.SilverJob"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[Mount(source=HOST_DATA_DIR, target="/app/data", type="bind")]
    )

    # Task 3: Execute the Gold Aggregation KPI Engine
    run_gold = DockerOperator(
        task_id='spark_gold_aggregation',
        image='retail-platform:latest',
        api_version='auto',
        auto_remove=True,
        command='sbt "runMain com.retail.gold.GoldJob"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[Mount(source=HOST_DATA_DIR, target="/app/data", type="bind")]
    )

    # Define the execution chain mapping sequence
    run_bronze >> run_silver >> run_gold