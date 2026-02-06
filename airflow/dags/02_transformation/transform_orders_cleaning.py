import pendulum
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

# Default arguments for the DAG
default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_orders_iceberg',
    description='Transform orders from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'orders'],
    default_args=default_args
)
def transform_orders_dag():
    """
    This DAG triggers a Spark job to:
    1. Read orders from Iceberg Raw table (iceberg.raw.orders).
    2. Convert timestamps to UTC+7.
    3. Cast currency columns to Decimal.
    4. Guard against negative prices.
    5. Save results to Iceberg Silver tables (iceberg.silver.orders) with history support.
    """

    transform_job = SparkSubmitOperator(
        task_id='spark_transform_orders',
        # Script location inside the container (shared via volume)
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        # Pass the logical execution date (YYYY-MM-DD) as an argument
        application_args=["{{ ds }}", "orders", path_manager.iceberg.raw.orders.get_table(), path_manager.iceberg.silver.orders.get_table()],
        # packages='org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,org.postgresql:postgresql:42.5.4,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026',

        # Ensure we have the necessary jars if they are not built into the image
        # Using packages might be slow on first run, but ensures portability.
        # But tabulario/spark-iceberg should already have these.
    )

    transform_job

# Initialize the DAG
dag_instance = transform_orders_dag()
