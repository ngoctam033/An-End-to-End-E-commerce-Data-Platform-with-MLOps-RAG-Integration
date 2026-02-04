import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_order_channel_iceberg',
    description='Transform order_channel from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'order_channel'],
    default_args=default_args
)
def transform_order_channel_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_order_channel',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "order_channel", path_manager.iceberg.raw.order_channel.get_table(), path_manager.iceberg.silver.order_channel.get_table()],

    )

    transform_job

dag_instance = transform_order_channel_dag()
