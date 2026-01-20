import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from utils.path_node import path_manager

def create_bucket_if_not_exists(bucket_name):
    """
    Hàm kiểm tra và tạo bucket trên MinIO nếu chưa tồn tại,
    sử dụng S3Hook của Airflow (tận dụng Connection minio_s3_conn).
    """
    # Khởi tạo S3Hook với connection ID đã cấu hình trong docker-compose
    hook = S3Hook(aws_conn_id='minio_s3_conn')

    try:
        # check_for_bucket trả về True nếu bucket tồn tại
        if not hook.check_for_bucket(bucket_name):
            logger.info(f">>> [S3Hook] Bucket '{bucket_name}' chưa tồn tại. Đang tạo mới...")
            hook.create_bucket(bucket_name=bucket_name)
            logger.info(f">>> [S3Hook] Bucket '{bucket_name}' đã được tạo thành công.")
        else:
            logger.info(f">>> [S3Hook] Bucket '{bucket_name}' đã tồn tại.")
            
    except Exception as err:
        logger.error(f">>> [S3Hook] Lỗi khi kiểm tra/tạo bucket: {err}")
        raise err
    
def ingest_postgres_to_minio():
    """
    Hàm thực hiện ETL:
    Hiện tại chỉ đơn giản là khởi tạo Spark Session để kiểm tra kết nối.
    """
    logger.info(">>> [ETL] Bắt đầu Job Ingest từ Postgres lên MinIO (Test Setup)...")
    create_bucket_if_not_exists("iceberg-warehouse")
    # 1. Khởi tạo Spark Session với cấu hình hỗ trợ S3 (MinIO)
    # Giữ lại các config này để đảm bảo môi trường Spark đã nhận diện được thư viện Hadoop AWS
    spark = SparkSession.builder \
        .appName("Ingest_Postgres_To_MinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        logger.info(f">>> [ETL] Spark Session tạo thành công! Version: {spark.version}")
        logger.info(">>> [ETL] Môi trường đã sẵn sàng. Chưa thực hiện logic đọc/ghi dữ liệu.")
        
    except Exception as e:
        logger.error(f">>> [ETL] Lỗi khi khởi tạo Spark: {str(e)}")
        raise e
    finally:
        spark.stop()
        logger.info(">>> [ETL] Đã đóng Spark Session.")

if __name__ == "__main__":
    ingest_postgres_to_minio()