from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- CẤU HÌNH KẾT NỐI ---
# Trong Airflow Admin -> Connections, bạn cần chỉnh sửa hoặc tạo mới connection này
# Conn Id: spark_remote_cluster
# Conn Type: Spark
# Host: spark://<TÊN_CONTAINER_SPARK_MASTER_CỦA_BẠN>:7077
# Ví dụ: spark://spark-master:7077 (Nếu chạy docker-compose chung network)
SPARK_CONN_ID = 'spark_remote_cluster'

# Đường dẫn file script TRÊN MÁY AIRFLOW
# Airflow sẽ đọc file này và gửi nội dung/logic sang cho Spark Cluster thực thi
SPARK_SCRIPT_PATH = '/opt/airflow/dags/scripts/ingest_iceberg.py'

# Các thư viện cần thiết (Phải khớp version với cụm Spark của bạn)
ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4"

# Cấu hình để Spark Master (Remote) có thể hiểu được MinIO
# Quan trọng: Endpoint phải là địa chỉ mà SPARK CONTAINER nhìn thấy, không phải localhost
SPARK_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    
    # Catalog Config
    "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.demo.type": "hadoop",
    "spark.sql.catalog.demo.warehouse": "s3a://datalake-warehouse/",
    
    # MinIO Config (Dành cho Remote Spark Cluster)
    # Lưu ý: http://minio:9000 phải là địa chỉ MinIO mà Spark Container có thể truy cập
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    
    # Tối ưu hóa cho việc submit từ xa
    "spark.network.timeout": "300s",
    "spark.executor.memory": "2g", # Executor chạy trên container Spark, nên set theo RAM của container đó
    "spark.driver.memory": "1g"    # Driver chạy trên Airflow, set nhỏ thôi để đỡ tốn RAM Airflow
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

TABLES = ['orders', 'products', 'customers']

with DAG(
    'remote_spark_submit_iceberg',
    default_args=default_args,
    description='Submit job từ Airflow sang Remote Spark Cluster',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['elt', 'remote-spark', 'iceberg'],
) as dag:

    for table in TABLES:
        submit_job = SparkSubmitOperator(
            task_id=f'remote_ingest_{table}',
            conn_id=SPARK_CONN_ID,
            
            application=SPARK_SCRIPT_PATH,
            
            # Tham số truyền vào script
            application_args=["{{ ds }}", table, "http://minio:9000"],
            
            packages=ICEBERG_PACKAGES,
            conf=SPARK_CONF,
            
            # QUAN TRỌNG: Client mode
            # Driver sẽ chạy tại Airflow (để log về Airflow UI)
            # Executor sẽ chạy tại Spark Cluster (để tính toán)
            deploy_mode='client',
            
            verbose=True
        )