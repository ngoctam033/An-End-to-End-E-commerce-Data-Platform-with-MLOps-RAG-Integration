import logging
import pendulum
from airflow.decorators import dag
from common.ingestion_tasks import extract_and_load_to_minio, ingest_task
from utils.path_node import path_manager
# from common.dag_registry import dag_registry
# Cấu hình logging
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_order_items_to_minio',
    description='Extract data from Postgres order_items and load to MinIO as Parquet',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'postgres', 'minio', 'parquet'],
    default_args=default_args
)
# # #
def load_order_items_to_minio_parquet():
    ds = ingest_task()
    # Thực thi task với các tham số cụ thể
    # order_items không có created_at nên thực hiện full load (date_column=None)
    extract_and_load_to_minio(
        table_name='order_items',
        bucket_name='datalake',
        extraction_date=ds,
        folder=path_manager.lakehouse.raw.order_items.get(),
        file_name = f"order_items_{ds}.parquet",
        date_column=None
    )

# Khởi tạo DAG
load_order_items_to_minio_parquet()
# dag_registry.register_dag('ingest_order_items_to_minio')