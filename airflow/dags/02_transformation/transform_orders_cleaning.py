import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='transform_orders_items_iceberg',
    description='Transform orders and order_items from raw Parquet to Iceberg Silver tables',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'iceberg', 'transformation', 'silver', 'orders'],
    default_args=default_args
)
def transform_orders_dag():
    """
    This DAG triggers a Spark job to:
    1. Read orders and order_items from raw Parquet files in MinIO.
    2. Convert timestamps to UTC+7.
    3. Cast currency columns to Decimal.
    4. Check data consistency between orders and items.
    5. Save results to Iceberg Silver tables with history support.
    """

    transform_job = SparkSubmitOperator(
        task_id='spark_transform_orders_items',
        # Script location inside the container (shared via volume)
        application='/opt/airflow/dags/scripts/transform_orders.py',
        conn_id='spark_default',
        # Pass the logical execution date (YYYY-MM-DD) as an argument
        application_args=["{{ ds }}"],
        # packages='org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,org.postgresql:postgresql:42.5.4,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026',
        conf={
            # --- CẤP PHÁT TÀI NGUYÊN (RESOURCE ALLOCATION) ---
            
            # 1. Tổng số cores tối đa mà toàn bộ ứng dụng này có thể chiếm dụng từ cluster
            'spark.cores.max': '2',
            
            # 2. Số cores cấp cho mỗi Executor (đơn vị xử lý song song)
            'spark.executor.cores': '1',
            
            # 3. Bộ nhớ RAM cấp cho mỗi Executor (ví dụ: 1g, 2g, 512m)
            'spark.executor.memory': '2g',
            
            # 4. Bộ nhớ RAM cấp cho Driver (nơi điều khiển job, chạy trong Airflow Worker)
            'spark.driver.memory': '1g',

            'spark.master': 'local',
            # Spark Networking
            'spark.submit.deployMode': 'client',
            'spark.driver.host': 'airflow-worker',
            'spark.driver.bindAddress': '0.0.0.0',
            
            # Iceberg with Hadoop Catalog
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': 'hadoop',
            'spark.sql.catalog.iceberg.warehouse': 's3a://datalake',
            'spark.sql.catalog.iceberg.s3.endpoint': 'http://minio1:9000',
            'spark.sql.catalog.iceberg.s3.access-key-id': 'admin',
            'spark.sql.catalog.iceberg.s3.secret-access-key': 'admin123',
            'spark.sql.catalog.iceberg.s3.path-style-access': 'true',
            'spark.sql.catalog.iceberg.s3.region': 'us-east-1',
            'spark.sql.catalog.iceberg.client.region': 'us-east-1',
            
            # AWS & S3A Environment/Hadoop Configs
            'spark.executorEnv.AWS_REGION': 'us-east-1',
            'spark.driverEnv.AWS_REGION': 'us-east-1',
            'spark.driver.extraJavaOptions': '-Daws.region=us-east-1',
            'spark.executor.extraJavaOptions': '-Daws.region=us-east-1',
            'spark.hadoop.aws.region': 'us-east-1',
            'spark.hadoop.fs.s3a.endpoint.region': 'us-east-1',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio1:9000',
            'spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'admin123',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.s3a.endpoint.region': 'us-east-1',

            # Environment for Executors
            'spark.executorEnv.AWS_REGION': 'us-east-1',
            'spark.driverEnv.AWS_REGION': 'us-east-1',

            # Extra Parquet/Iceberg Stability
            'spark.sql.parquet.enableVectorizedReader': 'false',
            'spark.sql.parquet.mergeSchema': 'false',
        },
        # Ensure we have the necessary jars if they are not built into the image
        # Using packages might be slow on first run, but ensures portability.
        # But tabulario/spark-iceberg should already have these.
    )

    transform_job

# Initialize the DAG
dag_instance = transform_orders_dag()
