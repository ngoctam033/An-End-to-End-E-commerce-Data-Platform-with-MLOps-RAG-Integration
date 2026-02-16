import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='build_daily_sales_mart',
    description='Build daily sales data mart (Gold layer) from Silver Iceberg tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'data_mart', 'gold', 'daily_sales'],
    default_args=default_args
)
def build_daily_sales_mart_dag():
    """
    Build the Daily Sales Mart:
    - Joins orders + order_items + customers + geo_location
    - Aggregates by date, province, order channel
    - Metrics: total_orders, total_revenue, total_profit, avg_order_value
    """
    build_job = SparkSubmitOperator(
        task_id='spark_build_daily_sales_mart',
        application='/opt/airflow/dags/scripts/build_data_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            "daily_sales_mart",
            path_manager.iceberg.gold.daily_sales_mart.get_table()
        ],
    )
    build_job

build_daily_sales_mart_dag()
