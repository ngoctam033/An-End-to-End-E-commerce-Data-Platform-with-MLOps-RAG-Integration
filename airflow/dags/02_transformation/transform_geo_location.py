import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_geo_location_iceberg',
    description='Transform geo_location from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'geo_location'],
    default_args=default_args
)
def transform_geo_location_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_geo_location',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "geo_location", path_manager.iceberg.raw.geo_location.get_table(), path_manager.iceberg.silver.geo_location.get_table()],
        verbose=True,
    )

    transform_job

dag_instance = transform_geo_location_dag()
