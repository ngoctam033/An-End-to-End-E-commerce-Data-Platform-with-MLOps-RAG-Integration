import logging
import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

# Cấu hình logging
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_customers_to_minio',
    description='Ingest customers from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'customers'],
    default_args=default_args
)
def ingest_customers_iceberg():
    """
    This DAG triggers a Spark job to:
    1. Read 'customers' table from Postgres via JDBC.
    2. Write directly to Iceberg Raw table in MinIO.
    """

    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_customers_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
        application_args=["customers", "{{ ds }}", "SELECT * FROM customers WHERE DATE(created_at) = '{{ ds }}'", path_manager.iceberg.raw.customers.get_table(), "id"],

    )

    ingest_job

ingest_customers_iceberg()
