import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_sub_category_iceberg',
    description='Transform sub_category from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'sub_category'],
    default_args=default_args
)
def transform_sub_category_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_sub_category',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "sub_category", path_manager.iceberg.raw.sub_category.get_table(), path_manager.iceberg.silver.sub_category.get_table()],

    )

    transform_job

dag_instance = transform_sub_category_dag()
