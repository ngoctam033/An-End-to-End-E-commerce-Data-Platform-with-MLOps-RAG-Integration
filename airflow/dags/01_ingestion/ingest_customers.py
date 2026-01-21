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
    'retries': 1,
}

@dag(
    dag_id='ingest_customers_to_minio',
    description='Extract data from Postgres customers and load to MinIO as Parquet',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'postgres', 'minio', 'parquet'],
    default_args=default_args
)
def load_customers_to_minio_parquet():
    ds = ingest_task()
    extract_and_load_to_minio(
        table_name='customers',
        bucket_name='datalake',
        extraction_date=ds,
        folder=path_manager.lakehouse.raw.customers.get(),
        file_name = f"customers_{ds}.parquet",
        date_column='created_at'
    )

load_customers_to_minio_parquet()
# dag_registry.register_dag('ingest_customers_to_minio')
