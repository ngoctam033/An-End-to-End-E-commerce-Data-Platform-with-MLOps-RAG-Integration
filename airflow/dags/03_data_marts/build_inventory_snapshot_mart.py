import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='build_inventory_snapshot_mart',
    description='Build inventory snapshot data mart (Gold layer) from Silver Iceberg tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'data_mart', 'gold', 'inventory'],
    default_args=default_args
)
def build_inventory_snapshot_mart_dag():
    """
    Build the Inventory Snapshot Mart:
    - Joins inventory + product + warehouse + geo_location
    - Computes: stock_value, is_low_stock, is_out_of_stock
    - Snapshot of current inventory status per warehouse/product
    """
    build_job = SparkSubmitOperator(
        task_id='spark_build_inventory_snapshot_mart',
        application='/opt/airflow/dags/scripts/build_data_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            "inventory_snapshot_mart",
            path_manager.iceberg.gold.inventory_snapshot_mart.get_table()
        ],
    )
    build_job

build_inventory_snapshot_mart_dag()
