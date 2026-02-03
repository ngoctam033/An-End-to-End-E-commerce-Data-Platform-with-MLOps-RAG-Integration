import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_sub_category_iceberg',
    description='Transform sub_category from Iceberg Raw to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'sub_category'],
    default_args=default_args
)
def transform_sub_category_dag():
    transform_job = SparkSubmitOperator(
        task_id='spark_transform_sub_category',
        application='/opt/airflow/dags/scripts/transform_table.py',
        conn_id='spark_default',
        application_args=["{{ ds }}", "sub_category"],
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
    )

    transform_job

dag_instance = transform_sub_category_dag()
