import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_order_channel_to_minio',
    description='Ingest order_channel from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'order_channel'],
    default_args=default_args
)
def ingest_order_channel_iceberg():
    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_order_channel_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
                application_args=["order_channel", "{{ ds }}", "SELECT * FROM order_channel", path_manager.iceberg.raw.order_channel.get_table()],

    )
    ingest_job

ingest_order_channel_iceberg()
