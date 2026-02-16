import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='build_logistics_mart',
    description='Build logistics & shipping analytics data mart (Gold layer) from Silver Iceberg tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'data_mart', 'gold', 'logistics'],
    default_args=default_args
)
def build_logistics_mart_dag():
    """
    Build the Logistics Mart:
    - Joins orders + shipment + logistics_partner + shipping_method + customers + geo_location
    - Aggregates by logistics partner, shipping method, province
    - Metrics: total_shipments, total/avg shipping cost, partner_rating, expedited_ratio
    """
    build_job = SparkSubmitOperator(
        task_id='spark_build_logistics_mart',
        application='/opt/airflow/dags/scripts/build_data_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            "logistics_mart",
            path_manager.iceberg.gold.logistics_mart.get_table()
        ],
    )
    build_job

build_logistics_mart_dag()
