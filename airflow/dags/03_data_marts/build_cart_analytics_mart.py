import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='build_cart_analytics_mart',
    description='Build cart analytics data mart (Gold layer) from Silver Iceberg tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'data_mart', 'gold', 'cart_analytics'],
    default_args=default_args
)
def build_cart_analytics_mart_dag():
    """
    Build the Cart Analytics Mart:
    - Joins cart + cart_items + customers + product
    - Metrics: abandonment rate, conversion rate, funnels by channel
    """
    build_job = SparkSubmitOperator(
        task_id='spark_build_cart_analytics_mart',
        application='/opt/airflow/dags/scripts/build_data_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            "cart_analytics_mart",
            path_manager.iceberg.gold.cart_analytics_mart.get_table()
        ],
    )
    build_job

build_cart_analytics_mart_dag()
