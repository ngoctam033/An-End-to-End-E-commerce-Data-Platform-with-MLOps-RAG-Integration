import logging
import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

# Cấu hình logging
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_product_review_to_minio',
    description='Ingest product_review from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'product_review'],
    default_args=default_args
)
def ingest_product_iceberg():
    """
    This DAG triggers a Spark job to:
    1. Read 'product_review' table from Postgres via JDBC.
    2. Filter by execution date (ds) using created_at.
    3. Write directly to Iceberg Raw table in MinIO.
    """

    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_product_review_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
        # Arguments: <table_name> <ds> <sql_query>
        application_args=["product_review", "{{ ds }}",
                            "SELECT * FROM product_review",
                            path_manager.iceberg.raw.product_review.get_table()],
        conf={
            # Resource Allocation
            'spark.cores.max': '1',
            'spark.executor.cores': '1',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '1g',

            # Iceberg with Hadoop Catalog
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': 'hadoop',
            'spark.sql.catalog.iceberg.warehouse': 's3a://datalake',
            
            # Hadoop/S3A Configs
            'spark.hadoop.fs.s3a.endpoint': 'http://minio1:9000',
            'spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'admin123',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        }
    )

    ingest_job

# Initialize the DAG
ingest_product_iceberg()
