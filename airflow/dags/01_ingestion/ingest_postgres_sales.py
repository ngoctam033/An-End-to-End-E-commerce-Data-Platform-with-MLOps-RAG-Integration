import logging
import pendulum
from airflow.decorators import dag
from common.ingestion_tasks import extract_and_load_to_minio, ingest_task
from utils.path_node import path_manager
# Cấu hình logging
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'ngoctam',
    'retries': 1,
}

@dag(
    dag_id='etl_postgres_to_minio_parquet',
    description='Extract data from Postgres and load to MinIO as Parquet',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'postgres', 'minio', 'parquet'],
    default_args=default_args
)
def load_orders_to_minio_parquet():
    ds = ingest_task()
    # Thực thi task với các tham số cụ thể
    extract_and_load_to_minio(
        table_name='orders', # Thay bằng tên bảng của bạn
        bucket_name='datalake',
        extraction_date=ds,
        folder=path_manager.lakehouse.raw.orders.get(),
        file_name = f"orders_{ds}.parquet"
    )

# Khởi tạo DAG
load_orders_to_minio_parquet()