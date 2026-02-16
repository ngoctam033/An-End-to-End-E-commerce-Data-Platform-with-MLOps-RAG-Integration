import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='build_product_performance_mart',
    description='Build product performance data mart (Gold layer) from Silver Iceberg tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'data_mart', 'gold', 'product_performance'],
    default_args=default_args
)
def build_product_performance_mart_dag():
    """
    Build the Product Performance Mart:
    - Joins order_items + product + sub_category + category + brand + product_review
    - Metrics: total_sold, total_revenue, avg_unit_price, avg_rating, total_reviews
    - Partitioned by category_name
    """
    build_job = SparkSubmitOperator(
        task_id='spark_build_product_performance_mart',
        application='/opt/airflow/dags/scripts/build_data_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            "product_performance_mart",
            path_manager.iceberg.gold.product_performance_mart.get_table()
        ],
    )
    build_job

build_product_performance_mart_dag()
