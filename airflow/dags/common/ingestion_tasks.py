import logging

import pandas as pd
import io
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.operators.python import get_current_context

from utils.path_node import PathManager

# Cấu hình logging
logger = logging.getLogger("airflow.task")

@task
def ingest_task():
    """
    Logic lấy context và thực thi phải nằm bên trong một @task
    """
    # Lấy context khi task đang thực thi
    context = get_current_context()
    ds = context['ds'] # Lấy chuỗi YYYY-MM-DD (ví dụ: 2026-01-19)
    return ds
@task
def extract_and_load_to_minio(table_name: str,
                              bucket_name: str,
                              extraction_date: datetime,
                              folder: PathManager,
                              file_name: str):
    """
    Quy trình: Extract (Postgres) -> Transform (Pandas to Parquet) -> Load (MinIO)
    Có xử lý ngoại lệ và raise lỗi để Airflow task failure.
    """
    
    # 1. Kết nối Postgres và Extract dữ liệu
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        # Sử dụng parameter binding để an toàn hơn thay vì f-string trực tiếp cho value
        sql = f"SELECT * FROM {table_name} WHERE DATE(created_at) = %s;"
        # date_str = extraction_date.strftime('%Y-%m-%d')
        logger.info(f"Trích xuất dữ liệu từ bảng {table_name} cho ngày {extraction_date}...")
        df = pg_hook.get_pandas_df(sql, parameters=(extraction_date,))

        if df.empty:
            logger.warning(f"Bảng {table_name} không có dữ liệu cho ngày {extraction_date}.")
            return

    except Exception as e:
        logger.error(f"Lỗi xảy ra trong quá trình Extract từ Postgres: {str(e)}")
        raise

    # 2. Transform: Chuyển đổi sang Parquet
    try:
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
    except Exception as e:
        logger.error(f"Lỗi xảy ra khi biến đổi dữ liệu sang Parquet: {str(e)}")
        raise

    # 3. Load: Upload lên MinIO bằng S3Hook
    try:
        object_path = f"{folder}/{file_name}"
        s3_hook = S3Hook(aws_conn_id='minio_default')

        # Kiểm tra và tạo bucket nếu cần
        if not s3_hook.check_for_bucket(bucket_name):
            logger.warning(f"Bucket '{bucket_name}' chưa tồn tại. Đang tạo mới...")
            s3_hook.create_bucket(bucket_name)

        s3_hook.load_file_obj(
            file_obj=parquet_buffer,
            key=object_path,
            bucket_name=bucket_name,
            replace=True
        )
        
    except Exception as e:
        logger.error(f"Lỗi xảy ra trong quá trình Load lên MinIO: {str(e)}")
        raise