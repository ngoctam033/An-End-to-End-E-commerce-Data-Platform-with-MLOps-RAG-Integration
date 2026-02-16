import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_cart_items_to_minio',
    description='Ingest cart_items from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'cart_items'],
    default_args=default_args
)
def ingest_cart_items_iceberg():
    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_cart_items_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
        application_args=["cart_items", "{{ ds }}", "SELECT * FROM cart_items", path_manager.iceberg.raw.cart_items.get_table()],
    )
    ingest_job

ingest_cart_items_iceberg()
