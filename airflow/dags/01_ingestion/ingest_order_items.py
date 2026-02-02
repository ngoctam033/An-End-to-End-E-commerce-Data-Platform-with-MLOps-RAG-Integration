import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_order_items_to_minio',
    description='Ingest order_items from Postgres to Iceberg Raw table using Spark',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'ingestion', 'raw', 'order_items'],
    default_args=default_args
)
def ingest_order_items_iceberg():
    """
    Triggers Spark job to ingest order_items to Iceberg Raw.
    order_items doesn't have a specific date column for incremental load, 
    but for this task we ingest it as it is.
    """
    ingest_job = SparkSubmitOperator(
        task_id='spark_ingest_order_items_to_minio',
        conn_id='spark_default',
        application='/opt/airflow/dags/scripts/ingest_table_to_iceberg.py',
        application_args=["order_items", "{{ ds }}", "SELECT * FROM order_items", ""],
        conf={
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
        }
    )

    ingest_job

ingest_order_items_iceberg()