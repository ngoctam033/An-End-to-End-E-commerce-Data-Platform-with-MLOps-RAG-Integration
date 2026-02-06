import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_inventory_iceberg',
    description='Transform inventory from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'inventory'],
    default_args=default_args
)
def transform_inventory_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_inventory',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "inventory", path_manager.iceberg.raw.inventory.get_table(), path_manager.iceberg.silver.inventory.get_table()],

    )

    transform_job

dag_instance = transform_inventory_dag()
