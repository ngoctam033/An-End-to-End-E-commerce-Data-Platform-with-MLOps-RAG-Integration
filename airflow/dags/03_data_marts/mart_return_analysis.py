from airflow.decorators import dag
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='mart_return_analysis',
    description='Build Gold Mart: Return Analysis (Rates, Reasons, Refund Impact)',
    schedule='0 2 * * *',  # Daily at 02:00 AM
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'gold', 'returns'],
    default_args=default_args
)
def mart_return_analysis_dag():
    
    computation_task = SparkSubmitOperator(
        task_id='compute_return_metrics',
        application='/opt/airflow/dags/03_data_marts/build_return_analysis_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            path_manager.iceberg.silver.order_return.get_table(),
            path_manager.iceberg.silver.orders.get_table(),
            path_manager.iceberg.silver.product.get_table(),
            "iceberg.gold.return_analysis"
        ]
    )

    computation_task

dag_instance = mart_return_analysis_dag()
