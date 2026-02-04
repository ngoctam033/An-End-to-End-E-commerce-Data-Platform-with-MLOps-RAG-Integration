import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.path_node import path_manager

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_order_items_iceberg',
    description='Transform order_items from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'order_items'],
    default_args=default_args
)
def transform_order_items_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_order_items',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "order_items", path_manager.iceberg.raw.order_items.get_table(), path_manager.iceberg.silver.order_items.get_table()],
        conf={
            'spark.cores.max': '4',
            'spark.executor.cores': '4',
            'spark.executor.memory': '8g',
            'spark.driver.memory': '2g',
            'spark.executor.memoryOverhead': '2g',
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
    )

    transform_job

dag_instance = transform_order_items_dag()
