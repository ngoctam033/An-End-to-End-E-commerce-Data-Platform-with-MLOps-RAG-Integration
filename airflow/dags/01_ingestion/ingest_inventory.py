import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_inventory_to_minio',
    description='Ingest inventory from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'inventory'],
    default_args=default_args
)
def ingest_inventory_iceberg():
    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_inventory_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
                application_args=["inventory", "{{ ds }}", "SELECT * FROM inventory", path_manager.iceberg.raw.inventory.get_table()],

    )
    ingest_job

ingest_inventory_iceberg()
