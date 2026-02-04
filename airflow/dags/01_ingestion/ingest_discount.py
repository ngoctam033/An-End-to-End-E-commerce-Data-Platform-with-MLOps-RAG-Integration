import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_discount_to_minio',
    description='Ingest discount from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'discount'],
    default_args=default_args
)
def ingest_discount_iceberg():
    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_discount_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
                application_args=["discount", "{{ ds }}", "SELECT * FROM discount", path_manager.iceberg.raw.discount.get_table()],

    )
    ingest_job

ingest_discount_iceberg()
