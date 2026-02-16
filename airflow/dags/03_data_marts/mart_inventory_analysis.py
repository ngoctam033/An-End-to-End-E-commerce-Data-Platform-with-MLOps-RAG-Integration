from airflow.decorators import dag, task
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='mart_inventory_analysis',
    description='Build Gold Mart: Inventory Analysis (Stock Levels, Turnover, Loss)',
    schedule='0 1 * * *',  # Daily at 01:00 AM
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'gold', 'inventory'],
    default_args=default_args
)
def mart_inventory_analysis_dag():
    
    computation_task = SparkSubmitOperator(
        task_id='compute_inventory_metrics',
        application='/opt/airflow/dags/03_data_marts/build_inventory_analysis_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            path_manager.iceberg.silver.inventory.get_table(),
            path_manager.iceberg.silver.inventory_log.get_table(),
            path_manager.iceberg.silver.warehouse.get_table(),
            path_manager.iceberg.silver.product.get_table(),
            "iceberg.gold.inventory_analysis"
        ]
    )

    computation_task

dag_instance = mart_inventory_analysis_dag()
