import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_product_review_iceberg',
    description='Transform product_review from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'product_review'],
    default_args=default_args
)
def transform_product_review_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_product_review',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "product_review",
                            path_manager.iceberg.raw.product_review.get_table(), 
                            path_manager.iceberg.silver.product_review.get_table()],

    )

    transform_job

dag_instance = transform_product_review_dag()
