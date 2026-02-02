import logging
import pendulum
from airflow.decorators import dag
from common.ingestion_tasks import extract_and_load_to_minio, ingest_task
from utils.path_node import path_manager

# Cấu hình logging
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'ngoctam',
    'retries': 0,
}

@dag(
    dag_id='ingest_product_review_to_minio',
    description='Extract data from Postgres product_review and load to MinIO as Parquet',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'postgres', 'minio', 'parquet'],
    default_args=default_args
)
def load_product_review_to_minio_parquet():
    ds = ingest_task()
    extract_and_load_to_minio(
        table_name='product_review',
        bucket_name='datalake',
        extraction_date=ds,
        folder=path_manager.lakehouse.raw.product_review.get(),
        file_name = f"product_review_{ds}.parquet",
        # date_column='created_at'
    )

load_product_review_to_minio_parquet()
