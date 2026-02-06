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
    dag_id='ingest_orders_to_minio',
    description='Ingest orders from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'orders'],
    default_args=default_args
)
def ingest_orders_iceberg():
    """
    This DAG triggers a Spark job to:
    1. Read 'orders' table from Postgres via JDBC.
    2. Filter by execution date (ds).
    3. Write directly to Iceberg Raw table in MinIO.
    """

    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_orders_to_minio',
        # Dùng connection spark_default đã config trong docker-compose
        conn_id='spark_default',
        # Script location inside the container
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
        # Arguments: <table_name> <ds> <sql_query> [primary_key]
        application_args=["orders", "{{ ds }}", "SELECT * FROM orders WHERE DATE(created_at) = '{{ ds }}'", path_manager.iceberg.raw.orders.get_table(), "id"],
        conf={
            # # Resource Allocation
            # 'spark.cores.max': '1',
            # 'spark.executor.cores': '1',
            # 'spark.executor.memory': '1g',
            # 'spark.driver.memory': '1g',

            # # Iceberg with Hadoop Catalog
            # 'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            # 'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            # 'spark.sql.catalog.iceberg.type': 'hadoop',
            # 'spark.sql.catalog.iceberg.warehouse': 's3a://datalake',
            
            # # Hadoop/S3A Configs (phải khớp với spark-worker)
            # 'spark.hadoop.fs.s3a.endpoint': 'http://minio1:9000',
            # 'spark.hadoop.fs.s3a.access.key': 'admin',
            # 'spark.hadoop.fs.s3a.secret.key': 'admin123',
            # 'spark.hadoop.fs.s3a.path.style.access': 'true',
            # 'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            # 'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            # 'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        }
    )

    ingest_job

# Initialize the DAG
ingest_orders_iceberg()