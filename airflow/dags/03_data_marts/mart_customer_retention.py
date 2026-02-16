from airflow.decorators import dag
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='mart_customer_retention',
    description='Build Gold Mart: Customer Retention (Churn, Engagement, Abandonment)',
    schedule='0 3 * * *',  # Daily at 03:00 AM
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'gold', 'customer', 'retention'],
    default_args=default_args
)
def mart_customer_retention_dag():
    
    computation_task = SparkSubmitOperator(
        task_id='compute_retention_metrics',
        application='/opt/airflow/dags/03_data_marts/build_customer_retention_mart.py',
        conn_id='spark_default',
        application_args=[
            "{{ ds }}",
            path_manager.iceberg.silver.customer_activity_log.get_table(),
            path_manager.iceberg.silver.orders.get_table(),
            "iceberg.gold.customer_retention"
        ]
    )

    computation_task

dag_instance = mart_customer_retention_dag()
