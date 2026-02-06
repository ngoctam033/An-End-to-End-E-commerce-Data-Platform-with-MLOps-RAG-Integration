import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_shipping_method_iceberg',
    description='Transform shipping_method from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'shipping_method'],
    default_args=default_args
)
def transform_shipping_method_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_shipping_method',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "shipping_method", path_manager.iceberg.raw.shipping_method.get_table(), path_manager.iceberg.silver.shipping_method.get_table()],

    )

    transform_job

dag_instance = transform_shipping_method_dag()
