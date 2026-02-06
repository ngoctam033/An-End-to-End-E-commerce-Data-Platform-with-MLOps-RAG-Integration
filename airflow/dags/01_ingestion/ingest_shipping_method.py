import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_shipping_method_to_minio',
    description='Ingest shipping_method from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'shipping_method'],
    default_args=default_args
)
def ingest_shipping_method_iceberg():
    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_shipping_method_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
                application_args=["shipping_method", "{{ ds }}", "SELECT * FROM shipping_method", path_manager.iceberg.raw.shipping_method.get_table()],

    )
    ingest_job

ingest_shipping_method_iceberg()
