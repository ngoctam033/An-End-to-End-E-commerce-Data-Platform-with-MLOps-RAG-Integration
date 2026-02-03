import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
        task_id='spark_transform_orders_items',
        # Script location inside the container (shared via volume)
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        # Pass the logical execution date (YYYY-MM-DD) as an argument
        application_args=["{{ ds }}"],
        # packages='org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,org.postgresql:postgresql:42.5.4,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026',
        conf={
            'spark.cores.max': '2',
            'spark.executor.cores': '1',
            'spark.executor.memory': '4g',
            'spark.driver.memory': '1g',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': 'hadoop',
            'spark.sql.catalog.iceberg.warehouse': 's3a://datalake',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio1:9000',
            'spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'admin123',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        },
        # Ensure we have the necessary jars if they are not built into the image
        # Using packages might be slow on first run, but ensures portability.
        # But tabulario/spark-iceberg should already have these.
    )

    transform_job

# Initialize the DAG
dag_instance = transform_orders_dag()
